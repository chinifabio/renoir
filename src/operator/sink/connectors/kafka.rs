use std::time::Duration;

use rdkafka::{
    producer::{BaseProducer, BaseRecord, Producer},
    ClientConfig,
};

use crate::{
    config::KafkaConfig,
    operator::{ExchangeData, StreamElement},
};

use super::ConnectorSinkStrategy;

pub struct KafkaSinkConnector<T: ExchangeData> {
    hosts: Vec<String>,
    producer: Option<BaseProducer>,
    topic: String,
    topic_key: Option<String>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: ExchangeData> Clone for KafkaSinkConnector<T> {
    fn clone(&self) -> Self {
        Self {
            hosts: self.hosts.clone(),
            producer: None,
            topic: self.topic.clone(),
            topic_key: self.topic_key.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: ExchangeData> std::fmt::Debug for KafkaSinkConnector<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSinkConnector")
            .field("hosts", &self.hosts)
            .field("topic", &self.topic)
            .field("topic_key", &self.topic_key)
            .finish()
    }
}

impl<T: ExchangeData> KafkaSinkConnector<T> {
    pub fn new(hosts: Vec<String>, topic: impl Into<String>) -> Self {
        Self {
            hosts,
            producer: None,
            topic: topic.into(),
            topic_key: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: ExchangeData> ConnectorSinkStrategy<T> for KafkaSinkConnector<T> {
    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", self.hosts.join(","))
            .set(
                "client.id",
                format!("renoir-sink-{}-{}", self.topic, metadata.global_id),
            )
            .create()
            .expect("Kafka producer creation failed");
        self.producer = Some(producer);
        self.topic_key = Some(format!("renoir-{}", metadata.global_id));
    }

    fn append(&mut self, item: &StreamElement<T>) {
        let json = serde_json::to_string(item).expect("Serialization failed");
        let record = BaseRecord::to(self.topic.as_str())
            .payload(&json)
            .key(self.topic_key.as_deref().expect("Topic key not set"));
        let producer = self
            .producer
            .as_mut()
            .expect("Kafka producer not configured");
        producer.send(record).expect("Kafka send failed");
        producer
            .flush(Duration::from_secs(1))
            .expect("Kafka flush failed");
    }

    fn technology(&self) -> String {
        "Kafka".to_string()
    }
}

impl<T: ExchangeData> From<&KafkaConfig> for KafkaSinkConnector<T> {
    fn from(value: &KafkaConfig) -> Self {
        KafkaSinkConnector::new(value.brokers.clone(), value.topic.clone())
    }
}
