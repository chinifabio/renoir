use rdkafka::{
    producer::{BaseProducer, BaseRecord, Producer},
    util::Timeout,
    ClientConfig,
};

use crate::{
    operator::{ExchangeData, StreamElement},
    CoordUInt,
};

use self::heartbeat::HeartbeatManager;

use super::{heartbeat, ConnectorSinkStrategy, GroupStreamElement};

const DEFAULT_FLUSH_TIMER: u32 = 32;

pub struct KafkaSinkConnector<T: ExchangeData> {
    hosts: Vec<String>,
    producer: Option<BaseProducer>,
    topic: String,
    replica_id: Option<CoordUInt>,
    flush_timer: u32,
    _phantom: std::marker::PhantomData<T>,

    heartbeat: HeartbeatManager,
    n_partition: usize,
}

impl<T: ExchangeData + std::fmt::Debug> std::fmt::Debug for KafkaSinkConnector<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSinkConnector")
            .field("hosts", &self.hosts)
            .field("topic", &self.topic)
            .field("replica_id", &self.replica_id)
            .field("flush_timer", &self.flush_timer)
            .field("_phantom", &self._phantom)
            .field("heartbeat", &self.heartbeat)
            .field("n_partition", &self.n_partition)
            .finish()
    }
}

impl<T: ExchangeData> Clone for KafkaSinkConnector<T> {
    fn clone(&self) -> Self {
        Self {
            hosts: self.hosts.clone(),
            producer: None,
            topic: self.topic.clone(),
            replica_id: None,
            flush_timer: self.flush_timer,
            _phantom: std::marker::PhantomData,

            heartbeat: self.heartbeat.clone(),
            n_partition: self.n_partition,
        }
    }
}

impl<T: ExchangeData> KafkaSinkConnector<T> {
    pub fn new(hosts: Vec<String>, topic: impl Into<String>, heartbeat: HeartbeatManager) -> Self {
        Self {
            hosts,
            producer: None,
            topic: topic.into(),
            replica_id: None,
            flush_timer: DEFAULT_FLUSH_TIMER,
            _phantom: std::marker::PhantomData,

            heartbeat,
            n_partition: 0,
        }
    }

    /// broad cast watermark to all partition. TODO presave the partition number to avoid overhead in communication
    fn broadcast_watermark(&mut self, item: &GroupStreamElement<T>) {
        let producer = self.producer.as_mut().expect("Missing producer");
        let json = serde_json::to_string(item).expect("Serialization failed");
        for p_id in 0..self.n_partition {
            let record = BaseRecord::to(self.topic.as_str())
                .payload(&json)
                .key(&[0]) // todo capire che chiave mettere
                .partition(p_id as i32);
            producer.send(record).expect("Kafka send failed");
            producer.flush(Timeout::Never).expect("Kafka flush failed");
        }
    }
}

impl<T: ExchangeData> ConnectorSinkStrategy<T> for KafkaSinkConnector<T> {
    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        let client_id = format!(
            "renoir-{}",
            metadata.group_name().as_deref().unwrap_or("default"),
        );

        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", self.hosts.join(","))
            .set("client.id", client_id)
            .create()
            .expect("Kafka producer creation failed");

        let topic_metadata = producer
            .client()
            .fetch_metadata(Some(self.topic.as_str()), Timeout::Never)
            .expect("Failed to fetch metadata");
        // this is because i assume each partition has a progressi id
        self.n_partition = topic_metadata.topics().first().unwrap().partitions().len();

        self.producer = Some(producer);
        self.replica_id = Some(metadata.global_id);

        if metadata.global_id == 0 {
            self.heartbeat.start_emitter(metadata.max_parallelism);
        }
    }

    fn append(&mut self, item: &GroupStreamElement<T>) {
        if let StreamElement::Watermark(t) = item.element {
            log::debug!("Broadcasting watermark with time {t}");
            self.broadcast_watermark(item);
            return;
        }
        let payload = serde_json::to_string(item).expect("Serialization failed");
        let key = self.replica_id.expect("Missing global id").to_string();
        let record = BaseRecord::to(self.topic.as_str())
            .payload(&payload)
            .key(key.as_str());
        let producer = self
            .producer
            .as_mut()
            .expect("Kafka producer not configured");
        producer.send(record).expect("Kafka send failed");
        producer.flush(Timeout::Never).expect("Kafka flush failed");
    }

    fn technology(&self) -> String {
        "Kafka".to_string()
    }
}
