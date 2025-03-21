use std::time::Duration;

use rdkafka::{
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
    ClientConfig,
};
use tokio::task::JoinHandle;

use crate::{config::KafkaConfig, operator::ExchangeData, CoordUInt};

use self::heartbeat::HeartbeatManager;

use super::{heartbeat, ConnectorSinkStrategy, GroupStreamElement};

const MAX_PARALLEL_SEND: usize = 2048;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct KafkaSinkConnector<T: ExchangeData> {
    #[derivative(Debug = "ignore")]
    producer: FutureProducer,
    topic: String,
    rt: tokio::runtime::Handle,
    _phantom: std::marker::PhantomData<T>,

    // these are used for the heartbeat
    replica_id: Option<CoordUInt>,
    heartbeat: HeartbeatManager,
    n_partition: usize,

    joinset: Option<Vec<JoinHandle<()>>>,
}

impl<T: ExchangeData + Clone> Clone for KafkaSinkConnector<T> {
    fn clone(&self) -> Self {
        Self {
            producer: self.producer.clone(),
            topic: self.topic.clone(),
            rt: self.rt.clone(),
            _phantom: self._phantom.clone(),
            replica_id: self.replica_id.clone(),
            heartbeat: self.heartbeat.clone(),
            n_partition: self.n_partition.clone(),
            joinset: None,
        }
    }
}

impl<T: ExchangeData> KafkaSinkConnector<T> {
    pub fn new(config: KafkaConfig, heartbeat: HeartbeatManager, group: String) -> Self {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", config.brokers.join(","))
            .set("client.id", format!("renoir-{}", group))
            .create()
            .expect("Kafka producer creation failed");

        Self {
            producer,
            topic: config.topic,
            rt: tokio::runtime::Handle::current(),
            _phantom: std::marker::PhantomData,

            replica_id: None,
            heartbeat,
            n_partition: 0,

            joinset: None,
        }
    }

    /// broad cast watermark to all partition. TODO presave the partition number to avoid overhead in communication
    #[allow(dead_code)]
    fn broadcast_watermark(&mut self, item: &GroupStreamElement<T>) {
        let _ = item;
        todo!("broadcast watermark to all partition");
    }

    /// Empty the joinset and wait for all tasks to finish
    pub fn wait(&mut self) {
        match self.joinset.as_mut() {
            Some(joinset) => {
                let mut joinset = std::mem::take(joinset);
                let (tx, rx) = flume::bounded(0);
                self.rt.spawn(async move {
                    for handle in joinset.drain(..) {
                        handle.await.expect("Kafka send failed");
                    }
                    tx.send(()).unwrap();
                });
                rx.recv().unwrap();
            }
            None => {
                log::warn!("KafkaSinkConnector has no joinset");
            }
        }
    }
}

impl<T: ExchangeData> ConnectorSinkStrategy<T> for KafkaSinkConnector<T> {
    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.joinset = Some(Vec::new());

        // this is because i assume each partition has a progressive id
        let topic_metadata = self
            .producer
            .client()
            .fetch_metadata(Some(self.topic.as_str()), Timeout::Never)
            .expect("Failed to fetch metadata");
        self.n_partition = topic_metadata.topics().first().unwrap().partitions().len();
        self.replica_id = Some(metadata.global_id);

        // if metadata.global_id == 0 {
        //     self.heartbeat.start_emitter(metadata.max_parallelism);
        // }
    }

    fn append(&mut self, item: &GroupStreamElement<T>) {
        let producer = self.producer.clone();
        let topic = self.topic.clone();

        let bytes = bincode::serialize(&item).expect("bincode serialization failed");
        let handle = self.rt.spawn(async move {
            let record = FutureRecord::to(&topic).key(&[]).payload(bytes.as_slice());

            producer
                .send(record, Timeout::After(Duration::from_secs(10)))
                .await
                .expect("kafka producer fail");
        });

        self.joinset.as_mut().expect("Missing joinset").push(handle);
        // if self.joinset.as_ref().unwrap().len() > MAX_PARALLEL_SEND {
        //     self.wait();
        // }
    }

    fn technology(&self) -> String {
        "Kafka".to_string()
    }
}

impl<T: ExchangeData> Drop for KafkaSinkConnector<T> {
    fn drop(&mut self) {
        // self.heartbeat.stop();
        self.wait();
    }
}
