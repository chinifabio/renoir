use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use flume::Receiver;
use futures::StreamExt;
use parking_lot::lock_api::Mutex;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::OwnedMessage,
    ClientConfig, Message,
};

use crate::{
    config::KafkaConfig,
    operator::{
        groups::{heartbeat::HeartbeatManager, GroupMap},
        ExchangeData, StreamElement,
    },
    Replication,
};

use super::{heartbeat::WatermarkManager, ConnectorSourceStrategy, GroupStreamElement};

enum KafkaSourceInner {
    Init {
        config: ClientConfig,
        topics: Vec<String>,
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
            Self::Init { config, topics } => Self::Init {
                config: config.clone(),
                topics: topics.clone(),
            },
            _ => panic!("can only clone KafkaSource in itialization state"),
        }
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct KafkaSourceConnector<T: ExchangeData> {
    #[derivative(Debug = "ignore")]
    inner: KafkaSourceInner,
    replication: Replication,
    _phantom: std::marker::PhantomData<T>,

    groups: GroupMap,
    heartbeat: HeartbeatManager,
    watermarks: WatermarkManager,
}

impl<T: ExchangeData> KafkaSourceConnector<T> {
    pub fn new(config: KafkaConfig, heartbeat: HeartbeatManager, group: String) -> Self {
        let groups = Arc::new(Mutex::new(HashMap::new()));
        let mut producer_config = ClientConfig::new();
        producer_config
            .set("bootstrap.servers", config.brokers.join(","))
            .set("group.id", format!("renoir-{}", group))
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest");
        match config.timeout {
            Some(timeout) => {
                producer_config.set("session.timeout.ms", (timeout * 1000).to_string());
            }
            None => {}
        };
        Self {
            inner: KafkaSourceInner::Init {
                config: producer_config,
                topics: vec![config.topic],
            },
            replication: Replication::Unlimited,
            _phantom: std::marker::PhantomData,

            groups: groups.clone(),
            heartbeat,
            watermarks: WatermarkManager::new(groups),
        }
    }
}

impl<T: ExchangeData> ConnectorSourceStrategy<T> for KafkaSourceConnector<T> {
    fn replication(&self) -> crate::Replication {
        crate::Replication::Unlimited
    }

    fn setup(&mut self, _metadata: &mut crate::ExecutionMetadata) {
        // I'm using replica_id since its different per host and I need a different Arc per host
        // if metadata.coord.replica_id == 0 {
        //     self.heartbeat
        //         .start_receiver(self.groups.clone(), metadata.global_id);
        // }

        let KafkaSourceInner::Init { config, topics } = &self.inner else {
            panic!("KafkaSource in invalid state")
        };

        let consumer: StreamConsumer = config.create().expect("Kafka consumer creation failed");
        let t = topics.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
        consumer
            .subscribe(t.as_slice())
            .expect("failed to subscribe to kafka topics");
        tracing::debug!("kafka source subscribed to {topics:?}");

        let (tx, rx) = flume::bounded(8);
        let cancel_token = Arc::new(AtomicBool::new(false));
        let cancel = cancel_token.clone();
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
                // consumer
                //     .commit_message(&msg, CommitMode::Async)
                //     .expect("kafka fail to commit");
            }
        });
        self.inner = KafkaSourceInner::Running {
            rx,
            cancel_token,
            cooldown: false,
        };
    }

    // TODO: aggiungere i flush batch, ecc..
    fn next(&mut self) -> GroupStreamElement<T> {
        match &mut self.inner {
            KafkaSourceInner::Init { .. } => {
                unreachable!("KafkaSource executing before setup!")
            }
            KafkaSourceInner::Running { rx, cooldown, .. } => loop {
                if *cooldown {
                    match rx.recv() {
                        Ok(msg) => {
                            *cooldown = false;
                            let payload = msg.payload().unwrap();
                            let item = bincode::deserialize(payload)
                                .expect("bincode deserialization failed");
                            return item;
                        }
                        Err(flume::RecvError::Disconnected) => {
                            tracing::warn!("kafka background task disconnected.");
                            return GroupStreamElement::wrap(StreamElement::Terminate, None, 0);
                        }
                    }
                }

                match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                    Ok(msg) => {
                        let payload = msg.payload().unwrap();
                        return bincode::deserialize(payload)
                            .expect("bincode deserialization failed");
                    }
                    Err(flume::RecvTimeoutError::Timeout) => {
                        *cooldown = true;
                        log::debug!("kafka source timeout");
                    }
                    Err(flume::RecvTimeoutError::Disconnected) => {
                        tracing::warn!("kafka background task disconnected.");
                        return GroupStreamElement::wrap(StreamElement::Terminate, None, 0);
                    }
                };
            },
        }
    }

    fn technology(&self) -> String {
        "Kafka".to_string()
    }
}

impl<T: ExchangeData> Drop for KafkaSourceConnector<T> {
    fn drop(&mut self) {
        match &mut self.inner {
            KafkaSourceInner::Init { .. } => {}
            KafkaSourceInner::Running { cancel_token, .. } => {
                cancel_token.store(true, Ordering::SeqCst);
            }
        }
    }
}
