use std::fmt::Display;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use flume::Receiver;
use futures::StreamExt;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::OwnedMessage;
use rdkafka::ClientConfig;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::operator::source::Source;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::Stream;

#[derive(Clone, Default)]
pub struct KafkaCommitRouter {
    routes: Arc<DashMap<i32, Arc<StreamConsumer>>>,
}

impl std::fmt::Debug for KafkaCommitRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaCommitRouter")
            .field("routes", &"some routes")
            .finish()
    }
}

impl KafkaCommitRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, partition: i32, consumer: Arc<StreamConsumer>) {
        self.routes.insert(partition, consumer);
    }

    pub fn unregister(&self, partition: i32) {
        self.routes.remove(&partition);
    }

    pub fn commit(&self, topic: &str, partition: i32, offset: i64) {
        if let Some(consumer) = self.routes.get(&partition) {
            if let Err(e) = consumer.store_offset(topic, partition, offset) {
                tracing::warn!("failed to store offset for partition {partition}: {e}");
            }
        } else {
            tracing::warn!("no registered consumer for partition {partition}, dropping commit");
        }
    }
}

enum KafkaSourceInner {
    Init {
        config: ClientConfig,
        topics: Vec<String>,
        router: Option<KafkaCommitRouter>,
    },
    Running {
        rx: Receiver<OwnedMessage>,
        cancel_token: Arc<AtomicBool>,
        cooldown: bool,
    }, // Terminated,
}

impl Clone for KafkaSourceInner {
    fn clone(&self) -> Self {
        match self {
            Self::Init {
                config,
                topics,
                router,
            } => Self::Init {
                config: config.clone(),
                topics: topics.clone(),
                router: router.clone(),
            },
            _ => panic!("can only clone KafkaSource in itialization state"),
        }
    }
}

/// # WARNING: KAFKA API IS EXPERIMENTAL
///
/// If replication is greater than `Replication::One` and timestamping logic
/// is being used, ensure that the number of kafka partitions receiving events
/// is greater than the number of replicas. Otherwise, watermarks may not be generated
/// stalling the computation. To solve this, reduce the replication.
///
/// TODO: address this
#[derive(Derivative)]
#[derivative(Debug)]
pub struct KafkaSource {
    #[derivative(Debug = "ignore")]
    inner: KafkaSourceInner,
    replication: Replication,
    terminated: bool,
}

impl Display for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KafkaSource")
    }
}

impl Source for KafkaSource {
    fn replication(&self) -> Replication {
        self.replication
    }
}

impl Operator for KafkaSource {
    type Out = rdkafka::message::OwnedMessage;

    fn setup(&mut self, _metadata: &mut ExecutionMetadata) {
        let KafkaSourceInner::Init {
            config,
            topics,
            router,
        } = &self.inner
        else {
            panic!("KafkaSource in invalid state")
        };

        let consumer = config
            .create::<StreamConsumer>()
            .expect("failed to create kafka consumer");
        let consumer = Arc::new(consumer);
        let t = topics.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
        consumer
            .subscribe(t.as_slice())
            .expect("failed to subscribe to kafka topics");
        tracing::debug!("kafka source subscribed to {topics:?}");

        if let Some(router) = router.clone() {
            consumer
                .assignment()
                .expect("failed to get assigned partitions")
                .elements()
                .iter()
                .for_each(|p| router.register(p.partition(), consumer.clone()));
        }

        let (tx, rx) = flume::bounded(8);
        let cancel_token = Arc::new(AtomicBool::new(false));
        let cancel = cancel_token.clone();
        let commit_flg = router.is_none();
        tracing::debug!("started kafka source with topics {:?}", topics);
        tokio::spawn(async move {
            let mut stream = consumer.stream();
            while let Some(msg) = stream.next().await {
                let msg = msg.expect("failed receiving from kafka");
                if cancel.load(Ordering::SeqCst) {
                    break;
                }
                let owned = msg.detach();
                if let Err(e) = tx.send(owned) {
                    if cancel.load(Ordering::SeqCst) {
                        break;
                    } else {
                        panic!("channel send failed for kafka source {e}");
                    }
                }
                if commit_flg {
                    consumer
                        .commit_message(&msg, CommitMode::Async)
                        .expect("kafka fail to commit");
                }
            }
        });
        self.inner = KafkaSourceInner::Running {
            rx,
            cancel_token,
            cooldown: false,
        };
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        match &mut self.inner {
            KafkaSourceInner::Init { .. } => {
                unreachable!("KafkaSource executing before setup!")
            }
            // KafkaSourceInner::Terminated => return StreamElement::Terminate,
            KafkaSourceInner::Running { rx, cooldown, .. } => {
                if *cooldown {
                    match rx.recv() {
                        Ok(msg) => {
                            *cooldown = false;
                            return StreamElement::Item(msg);
                        }
                        Err(flume::RecvError::Disconnected) => {
                            tracing::warn!("kafka background task disconnected.");
                            return StreamElement::Terminate;
                        }
                    }
                }

                match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                    Ok(msg) => StreamElement::Item(msg),
                    Err(flume::RecvTimeoutError::Timeout) => {
                        *cooldown = true;
                        StreamElement::FlushBatch
                    }
                    Err(flume::RecvTimeoutError::Disconnected) => {
                        tracing::warn!("kafka background task disconnected.");
                        StreamElement::Terminate
                    } // StreamElement::Terminate,
                }
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Self::Out, _>("KafkaSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl Clone for KafkaSource {
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        if matches!(self.replication, Replication::Host | Replication::One) {
            panic!(
                "KafkaSource with replication {:?} cannot be cloned",
                self.replication
            );
        }

        Self {
            inner: self.inner.clone(),
            replication: self.replication,
            terminated: false,
        }
    }
}

impl Drop for KafkaSource {
    fn drop(&mut self) {
        match &self.inner {
            KafkaSourceInner::Init { .. } => {}
            KafkaSourceInner::Running { cancel_token, .. } => {
                cancel_token.store(true, Ordering::SeqCst);
            }
        }
    }
}

impl crate::StreamContext {
    /// Convenience method, creates a `KafkaSource` and makes a stream using `StreamContext::stream`
    ///
    /// See Examples
    ///
    /// # WARNING: KAFKA API IS EXPERIMENTAL
    ///
    /// If replication is greater than `Replication::One` and timestamping logic
    /// is being used, ensure that the number of kafka partitions receiving events
    /// is greater than the number of replicas. Otherwise, watermarks may not be generated
    /// stalling the computation. To solve this, reduce the replication.
    ///
    /// TODO: address this
    pub fn stream_kafka(
        &self,
        client_config: ClientConfig,
        topics: &[&str],
        replication: Replication,
    ) -> Stream<KafkaSource> {
        let source = KafkaSource {
            inner: KafkaSourceInner::Init {
                config: client_config,
                topics: topics.iter().map(|s| s.to_string()).collect(),
                router: None,
            },
            replication,
            terminated: false,
        };
        self.stream(source)
    }

    /// Create a `KafkaSource` and a channel to commit offsets to.
    ///
    /// Offsets are committed asynchronously using a `KafkaCommitRouter`.`
    /// The channel has a buffer of `channel_size` elements. If the buffer is full, `send` will block.
    ///
    /// # WARNING: KAFKA API IS EXPERIMENTAL
    ///
    /// If replication is greater than `Replication::One` and timestamping logic
    /// is being used, ensure that the number of kafka partitions receiving events
    /// is greater than the number of replicas. Otherwise, watermarks may not be generated
    /// stalling the computation. To solve this, reduce the replication.
    ///
    /// TODO: address this
    pub fn stream_kafka_with_commit_router(
        &self,
        client_config: ClientConfig,
        topics: &[&str],
        replication: Replication,
    ) -> (Stream<KafkaSource>, KafkaCommitRouter) {
        let router = KafkaCommitRouter::new();
        let source = KafkaSource {
            inner: KafkaSourceInner::Init {
                config: client_config,
                topics: topics.iter().map(|s| s.to_string()).collect(),
                router: Some(router.clone()),
            },
            replication,
            terminated: false,
        };
        (self.stream(source), router)
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use futures::StreamExt;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::message::Message;
    use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
    use rdkafka::ClientConfig;

    use crate::operator::source::KafkaCommitRouter;
    use crate::prelude::*;

    static GRP_COUNTER: AtomicUsize = AtomicUsize::new(0);

    async fn mock_kafka() -> (FutureProducer, StreamConsumer) {
        const TOPIC: &str = "test_topic";
        let producer: FutureProducer = ClientConfig::new()
            .set("test.mock.num.brokers", "1")
            .create()
            .expect("Producer creation error");

        let bootstrap_servers = {
            let mock_cluster = producer.client().mock_cluster().unwrap();
            let _ = mock_cluster.create_topic(TOPIC, 1, 1);
            mock_cluster.bootstrap_servers()
        };

        let group_id = format!("mock-grp-{}", GRP_COUNTER.fetch_add(1, Ordering::SeqCst));
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", &group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .create()
            .expect("Client creation error");

        let rec = FutureRecord::to(TOPIC).key("msg1").payload("test");
        producer.send_result(rec).unwrap().await.unwrap().unwrap();

        consumer.subscribe(&[TOPIC]).unwrap();

        (producer, consumer)
    }

    fn bootstrap_servers(producer: &FutureProducer) -> String {
        let mock_cluster = producer.client().mock_cluster().unwrap();
        mock_cluster.bootstrap_servers()
    }

    #[tokio::test]
    async fn test_mock_kafka_basic() {
        let (producer, consumer) = mock_kafka().await;
        let mut stream = consumer.stream();
        let msg = stream.next().await.unwrap().unwrap();
        let payload = msg.payload_view::<str>().unwrap().unwrap();
        assert_eq!(payload, "test");
        drop(producer);
    }

    #[tokio::test]
    async fn test_commit_router_register_and_commit() {
        const TOPIC: &str = "test_topic";
        let router = KafkaCommitRouter::new();
        assert_eq!(
            format!("{router:?}"),
            "KafkaCommitRouter { routes: \"some routes\" }"
        );

        // Commit on unregistered partition should drop cleanly without panic
        router.commit(TOPIC, 0, 0);

        let (producer, consumer) = mock_kafka().await;
        let consumer_arc = Arc::new(consumer);
        let mut stream = consumer_arc.stream();

        let (partition, offset) = {
            let msg = stream.next().await.unwrap().unwrap();
            (msg.partition(), msg.offset())
        };

        // Register partition and commit
        router.register(partition, consumer_arc.clone());
        router.commit(TOPIC, partition, offset);

        router.unregister(partition);
        drop(stream);
        drop(router);
        drop(consumer_arc);
        drop(producer);
    }

    #[tokio::test]
    async fn test_commit_router_unregister() {
        const TOPIC: &str = "test_topic";
        let router = KafkaCommitRouter::new();
        let (producer, consumer) = mock_kafka().await;
        let consumer_arc = Arc::new(consumer);

        router.register(0, consumer_arc);
        router.commit(TOPIC, 0, 10);

        router.unregister(0);
        // After unregistering, partition 0 is no longer mapped
        router.commit(TOPIC, 0, 10);
        drop(producer);
    }

    #[tokio::test]
    async fn test_commit_router_cloning() {
        const TOPIC: &str = "test_topic";
        let router1 = KafkaCommitRouter::new();
        let router2 = router1.clone();

        let (producer, consumer) = mock_kafka().await;
        let consumer_arc = Arc::new(consumer);

        // Register via router1
        router1.register(0, consumer_arc);

        // router2 shares the underlying DashMap routes
        router2.commit(TOPIC, 0, 5);

        // Unregister via router2
        router2.unregister(0);

        // router1 should also see it unregistered
        router1.commit(TOPIC, 0, 5);
        drop(producer);
    }

    #[tokio::test]
    async fn test_commit_router_multithreaded_concurrent() {
        let router = KafkaCommitRouter::new();
        let (producer, _) = mock_kafka().await;
        let b = bootstrap_servers(&producer);

        let mut handles = vec![];
        for i in 0..8 {
            let r = router.clone();
            let b = b.clone();
            handles.push(tokio::spawn(async move {
                let mut config = ClientConfig::new();
                config.set("bootstrap.servers", &b);
                config.set("group.id", format!("test-grp-{i}"));
                let consumer: StreamConsumer = config.create().unwrap();
                let consumer_arc = Arc::new(consumer);

                r.register(i, consumer_arc);
                for offset in 0..20 {
                    r.commit("test_topic", i, offset);
                }
                r.unregister(i);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_stream_kafka_with_commit_router_pipeline_build() {
        let (producer, _) = mock_kafka().await;
        let b = bootstrap_servers(&producer);

        let ctx = StreamContext::new_local();
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &b);
        config.set("group.id", "test-build-grp");

        let (stream, router) = ctx.stream_kafka_with_commit_router(
            config,
            &["test_topic"],
            Replication::One,
        );

        // Verify stream can chain operators
        let _ = stream
            .map(|msg| msg.offset())
            .filter(|offset| *offset >= 0);

        assert_eq!(
            format!("{router:?}"),
            "KafkaCommitRouter { routes: \"some routes\" }"
        );
    }
}

