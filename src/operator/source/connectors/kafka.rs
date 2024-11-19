use std::{io::Read, time::Duration};

use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    util::Timeout,
    ClientConfig, Message,
};

use crate::{
    config::KafkaConfig,
    operator::{ExchangeData, StreamElement},
};

use super::{ConnectorSource, ConnectorSourceStrategy};

pub struct KafkaSourceConnector<T: ExchangeData> {
    hosts: Vec<String>,
    consumer: Option<BaseConsumer>,
    topic: String,
    _phantom: std::marker::PhantomData<T>,
    timeout: Timeout,
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
            timeout: self.timeout,
        }
    }
}

impl<T: ExchangeData> KafkaSourceConnector<T> {
    pub fn new(hosts: Vec<String>, topic: impl Into<String>, timeout: Option<u64>) -> Self {
        let timeout = match timeout {
            Some(timeout) => Timeout::After(Duration::from_secs(timeout)),
            None => Timeout::Never,
        };

        Self {
            hosts,
            consumer: None,
            topic: topic.into(),
            _phantom: std::marker::PhantomData,
            timeout,
        }
    }
}

impl<T: ExchangeData> ConnectorSourceStrategy<T> for KafkaSourceConnector<T> {
    fn replication(&self) -> crate::Replication {
        crate::Replication::Unlimited
    }

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        let group = match metadata.group.as_deref() {
            Some(group_name) => group_name,
            None => {
                log::warn!("No groups specified for Kafka source, using 'default'");
                "default"
            }
        };

        let consumer_group = match metadata.group_replica.as_deref() {
            Some(group_name) => {
                format!("renoir-{}-{}", group, group_name)
            }
            None => format!("renoir-{}", group),
        };

        log::info!("Creating Kafka consumer with group id: {}", consumer_group);
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.hosts.join(","))
            .set("group.id", consumer_group)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Kafka consumer creation failed");
        consumer
            .subscribe(&[self.topic.as_str()])
            .expect("Kafka consumer subscription failed");
        self.consumer = Some(consumer);
    }

    fn next(&mut self) -> StreamElement<T> {
        let consumer = self
            .consumer
            .as_mut()
            .expect("Kafka consumer not configured");

        loop {
            match consumer.poll(self.timeout) {
                Some(Ok(message)) => {
                    let mut payload = message.payload().unwrap();
                    let mut buffer = Vec::new();
                    payload.read_to_end(&mut buffer).unwrap();
                    let json = String::from_utf8(buffer).unwrap();
                    return serde_json::from_str(&json).expect("Failed to parse JSON");
                }
                Some(Err(e)) => {
                    log::error!("Kafka message error: {:?}", e);
                    panic!("some error occurred while consuming kafka message, todo: should i stay or should i go?");
                }
                None => {
                    log::warn!("Ignoring empty message from Kafka");
                }
            }
        }
    }

    fn technology(&self) -> String {
        "Kafka".to_string()
    }
}

impl<T: ExchangeData> From<&KafkaConfig> for KafkaSourceConnector<T> {
    fn from(value: &KafkaConfig) -> Self {
        KafkaSourceConnector::new(value.brokers.clone(), value.topic.clone(), value.timeout)
    }
}

impl crate::StreamContext {
    pub fn stream_from_kafka<T: ExchangeData>(
        &self,
        brokers: Vec<String>,
        topic: impl Into<String>,
        timeout: Option<u64>,
    ) -> crate::Stream<ConnectorSource<T>> {
        let kafka_strategy = KafkaSourceConnector::new(brokers, topic, timeout);
        self.stream(ConnectorSource::new_remote(
            super::ConnectorSourceTechnology::Kafka(kafka_strategy),
        ))
    }
}
