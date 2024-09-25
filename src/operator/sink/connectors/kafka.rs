use std::time::Duration;

use rdkafka::{
    producer::{BaseProducer, BaseRecord, Producer},
    ClientConfig,
};

use crate::{config::KafkaConfig, operator::ExchangeData};

use super::ConnectorSinkStrategy;

pub struct KafkaSinkConnector<T: ExchangeData> {
    hosts: Vec<String>,
    producer: Option<BaseProducer>,
    topic: String,
    final_topic: Option<String>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: ExchangeData> Clone for KafkaSinkConnector<T> {
    fn clone(&self) -> Self {
        Self {
            hosts: self.hosts.clone(),
            producer: None,
            topic: self.topic.clone(),
            final_topic: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: ExchangeData> std::fmt::Debug for KafkaSinkConnector<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSinkConnector")
            .field("hosts", &self.hosts)
            .field("topic", &self.topic)
            .field("final_topic", &self.final_topic)
            .finish()
    }
}

impl<T: ExchangeData> KafkaSinkConnector<T> {
    pub fn new(hosts: Vec<String>, topic: impl Into<String>) -> Self {
        Self {
            hosts,
            producer: None,
            topic: topic.into(),
            final_topic: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: ExchangeData> ConnectorSinkStrategy<T> for KafkaSinkConnector<T> {
    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.final_topic = Some(format!("{}-{}", self.topic, metadata.global_id));
        let producer = ClientConfig::new()
            .set("bootstrap.servers", self.hosts.join(","))
            .create()
            .expect("Kafka producer creation failed");
        self.producer = Some(producer);
    }

    fn append(&mut self, item: &crate::operator::StreamElement<T>) {
        let json = serde_json::to_string(item).unwrap();
        let record = BaseRecord::to(self.final_topic.as_ref().unwrap())
            .payload(&json)
            .key("gino");
        self.producer
            .as_mut()
            .expect("Kafka producer not configured")
            .send(record)
            .expect("Kafka send failed");
        self.producer
            .as_mut()
            .expect("Kafka producer not configured")
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
