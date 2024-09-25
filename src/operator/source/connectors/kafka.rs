use std::io::Read;

use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    util::Timeout,
    ClientConfig, Message,
};

use crate::{config::KafkaConfig, operator::ExchangeData};

use super::ConnectorSourceStrategy;

pub struct KafkaSourceConnector<T: ExchangeData> {
    hosts: Vec<String>,
    consumer: Option<BaseConsumer>,
    topic: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: ExchangeData> std::fmt::Debug for KafkaSourceConnector<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSourceConnector")
            .field("hosts", &self.hosts)
            .field("topic", &self.topic)
            .finish()
    }
}

impl<T: ExchangeData> Clone for KafkaSourceConnector<T> {
    fn clone(&self) -> Self {
        Self {
            hosts: self.hosts.clone(),
            consumer: None,
            topic: self.topic.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: ExchangeData> KafkaSourceConnector<T> {
    pub fn new(hosts: Vec<String>, topic: impl Into<String>) -> Self {
        Self {
            hosts,
            consumer: None,
            topic: topic.into(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: ExchangeData> ConnectorSourceStrategy<T> for KafkaSourceConnector<T> {
    fn replication(&self) -> crate::Replication {
        crate::Replication::Unlimited
    }

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        let final_topic = format!("{}-{}", self.topic, metadata.global_id);
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.hosts.join(","))
            .set("group.id", "renoir-source")
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Kafka consumer creation failed");
        consumer
            .subscribe(&[final_topic.as_str()])
            .expect("Kafka consumer subscription failed");
        self.consumer = Some(consumer);
    }

    fn next(&mut self) -> crate::operator::StreamElement<T> {
        let consumer = self
            .consumer
            .as_mut()
            .expect("Kafka consumer not configured");
        match consumer.poll(Timeout::Never) {
            Some(Ok(message)) => {
                let mut payload = message.payload().unwrap();
                let mut buffer = Vec::new();
                payload.read_to_end(&mut buffer).unwrap();
                let json = String::from_utf8(buffer).unwrap();
                serde_json::from_str(&json).unwrap()
            }
            Some(Err(e)) => {
                panic!("Kafka message error: {}", e);
            }
            None => {
                panic!("Kafka message timeout");
            }
        }
    }

    fn technology(&self) -> String {
        "Kafka".to_string()
    }
}

impl<T: ExchangeData> From<&KafkaConfig> for KafkaSourceConnector<T> {
    fn from(value: &KafkaConfig) -> Self {
        KafkaSourceConnector::new(value.brokers.clone(), value.topic.clone())
    }
}
