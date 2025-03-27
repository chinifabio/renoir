use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::StreamExt;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
    ClientConfig, Message,
};

use crate::{
    config::KafkaConfig,
    flowunits::{MessageMetadata, RenoirMessage},
    operator::{ExchangeData, StreamElement},
};

use super::LayerChannelExt;

const MAX_PARALLEL_SEND: usize = 2048;

struct KafkaConsumer<T: ExchangeData> {
    rx: flume::Receiver<RenoirMessage<T>>,
    // TODO wait nel drop
    #[allow(dead_code)]
    cancel_token: Arc<AtomicBool>,
}

struct KafkaProducer<T: ExchangeData> {
    producer: FutureProducer,
    topic: String,
    rt: tokio::runtime::Handle,
    joinset: Vec<tokio::task::JoinHandle<()>>,
    n_partition: usize,
    _phantom: std::marker::PhantomData<T>,
}

enum KafkaChannelInner<T: ExchangeData> {
    Producer(KafkaProducer<T>),
    Consumer(KafkaConsumer<T>),
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct KafkaChannel<T: ExchangeData> {
    config: KafkaConfig,
    #[derivative(Debug = "ignore")]
    inner: KafkaChannelInner<T>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: ExchangeData> KafkaChannel<T> {
    pub fn new_producer(config: KafkaConfig, layer: String) -> Self {
        let inner = KafkaChannelInner::Producer(KafkaProducer::new(config.clone(), layer));

        Self {
            config,
            inner,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn new_consumer(config: KafkaConfig, layer: String) -> Self {
        let inner = KafkaChannelInner::Consumer(KafkaConsumer::new(config.clone(), layer));

        Self {
            config,
            inner,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: ExchangeData> LayerChannelExt<T> for KafkaChannel<T> {
    fn send(&mut self, metadata: &MessageMetadata, item: &StreamElement<T>) {
        match &mut self.inner {
            KafkaChannelInner::Producer(channel) => channel.send(metadata, item),
            KafkaChannelInner::Consumer(_) => panic!("Cannot send to a consumer"),
        }
    }

    fn broadcast(&mut self, metadata: &MessageMetadata, item: &StreamElement<T>) {
        match &mut self.inner {
            KafkaChannelInner::Producer(channel) => channel.broadcast(metadata, item),
            KafkaChannelInner::Consumer(_) => panic!("Cannot send to a consumer"),
        }
    }

    fn recv(&mut self) -> Option<RenoirMessage<T>> {
        match &mut self.inner {
            KafkaChannelInner::Producer(_) => panic!("Cannot receive from a producer"),
            KafkaChannelInner::Consumer(channel) => channel.recv(),
        }
    }

    fn recv_timeout(&mut self, timeout: std::time::Duration) -> Option<RenoirMessage<T>> {
        match &mut self.inner {
            KafkaChannelInner::Producer(_) => panic!("Cannot receive from a producer"),
            KafkaChannelInner::Consumer(channel) => channel.recv_timeout(timeout),
        }
    }
}

impl<T: ExchangeData> KafkaProducer<T> {
    fn new(config: KafkaConfig, layer: String) -> Self {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", config.brokers.join(","))
            .set("client.id", format!("renoir-{}", layer))
            .create::<FutureProducer>()
            .expect("Kafka producer creation failed");

        let topic_metadata = producer
            .client()
            .fetch_metadata(Some(config.topic.as_str()), Timeout::Never)
            .expect("Failed to fetch metadata");
        let n_partition = topic_metadata.topics().first().unwrap().partitions().len();

        Self {
            joinset: Vec::new(),
            producer,
            topic: config.topic,
            rt: tokio::runtime::Handle::current(),
            n_partition,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Empty the joinset and wait for all tasks to finish
    fn wait(&mut self) {
        let mut joinset = std::mem::take(&mut self.joinset);
        let (tx, rx) = flume::bounded(0);
        self.rt.spawn(async move {
            for handle in joinset.drain(..) {
                handle.await.expect("Kafka send failed");
            }
            tx.send(()).unwrap();
        });
        rx.recv().unwrap();
    }

    fn send(&mut self, metadata: &MessageMetadata, item: &StreamElement<T>) {
        log::debug!("Sending to kafka: {}", item.variant_str());
        let producer = self.producer.clone();
        let topic = self.topic.clone();

        let bytes = serde_json::to_string(&(metadata, item)).expect("bincode serialization failed");
        let handle = self.rt.spawn(async move {
            let record: FutureRecord<[u8], [u8]> =
                FutureRecord::to(&topic).payload(bytes.as_bytes());
            producer
                .send(record, Timeout::After(Duration::from_secs(10)))
                .await
                .expect("kafka producer fail");
        });

        self.joinset.push(handle);
        if self.joinset.len() > MAX_PARALLEL_SEND {
            self.wait();
        }
    }

    fn broadcast(&mut self, metadata: &MessageMetadata, item: &StreamElement<T>) {
        for partition in 0..self.n_partition {
            let producer = self.producer.clone();
            let topic = self.topic.clone();

            let bytes =
                serde_json::to_string(&(metadata, item)).expect("bincode serialization failed");
            let handle = self.rt.spawn(async move {
                let record: FutureRecord<[u8], [u8]> = FutureRecord::to(&topic)
                    .payload(bytes.as_bytes())
                    .partition(partition as i32);

                producer
                    .send(record, Timeout::After(Duration::from_secs(10)))
                    .await
                    .expect("kafka producer fail");
            });

            self.joinset.push(handle);
        }

        if self.joinset.len() > MAX_PARALLEL_SEND {
            self.wait();
        }
    }
}

impl<T: ExchangeData> KafkaConsumer<T> {
    fn new(config: KafkaConfig, layer: String) -> Self {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", config.brokers.join(","))
            .set("group.id", format!("renoir-{}", layer))
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create::<StreamConsumer>()
            .expect("Kafka consumer creation failed");

        consumer
            .subscribe(&[config.topic.as_str()])
            .expect("Failed to subscribe");

        let (tx, rx) = flume::unbounded();
        let cancel_token = Arc::new(AtomicBool::new(false));
        let cancel = cancel_token.clone();
        tokio::spawn(async move {
            let mut stream = consumer.stream();
            while let Some(msg) = stream.next().await {
                let msg = msg.expect("failed receiving from kafka");
                if cancel.load(Ordering::SeqCst) {
                    break;
                }
                let owned = msg.detach();
                let bytes = owned.payload().unwrap();
                log::debug!("Received bytes: {}", String::from_utf8_lossy(bytes));
                let message: RenoirMessage<T> =
                    serde_json::from_slice(bytes).expect("bincode deserialization failed");
                if let Err(e) = tx.send(message) {
                    if cancel.load(Ordering::SeqCst) {
                        break;
                    } else {
                        panic!("channel send failed for kafka source {e}");
                    }
                }
            }
        });

        Self { rx, cancel_token }
    }

    #[allow(dead_code)]
    fn recv(&mut self) -> Option<RenoirMessage<T>> {
        self.rx.try_recv().ok()
    }

    fn recv_timeout(&mut self, timeout: std::time::Duration) -> Option<RenoirMessage<T>> {
        self.rx
            .recv_timeout(timeout)
            .inspect_err(|err| {
                log::error!("Kafka consumer error: {}", err);
                panic!()
            })
            .ok()
    }
}
