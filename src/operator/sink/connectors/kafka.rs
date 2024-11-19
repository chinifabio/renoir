use rdkafka::{
    producer::{BaseProducer, BaseRecord, Producer},
    util::Timeout,
    ClientConfig,
};

use crate::{
    config::KafkaConfig,
    operator::{ExchangeData, StreamElement},
};

use super::ConnectorSinkStrategy;

const DEFAULT_FLUSH_TIMER: u32 = 32;

pub struct KafkaSinkConnector<T: ExchangeData> {
    hosts: Vec<String>,
    producer: Option<BaseProducer>,
    topic: String,
    topic_key: Option<String>,
    _phantom: std::marker::PhantomData<T>,
    flush_timer: u32,
}

impl<T: ExchangeData> Clone for KafkaSinkConnector<T> {
    fn clone(&self) -> Self {
        Self {
            hosts: self.hosts.clone(),
            producer: None,
            topic: self.topic.clone(),
            topic_key: self.topic_key.clone(),
            _phantom: std::marker::PhantomData,
            flush_timer: self.flush_timer,
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
            flush_timer: DEFAULT_FLUSH_TIMER,
        }
    }
}

impl<T: ExchangeData> ConnectorSinkStrategy<T> for KafkaSinkConnector<T> {
    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        let group = match metadata.group.as_deref() {
            Some(group_name) => group_name,
            None => {
                log::warn!("No groups specified for Kafka sink, using 'default'");
                "default"
            }
        };

        let client_id = match metadata.group_replica.as_deref() {
            Some(group_name) => {
                format!("renoir-{}-{}-{}", group, group_name, metadata.global_id)
            }
            None => {
                format!("renoir-{}-{}", group, metadata.global_id)
            }
        };

        let producer = ClientConfig::new()
            .set("bootstrap.servers", self.hosts.join(","))
            .set("client.id", client_id.clone())
            .create()
            .expect("Kafka producer creation failed");

        self.producer = Some(producer);
        self.topic_key = Some(client_id);
    }

    fn append(&mut self, item: &StreamElement<T>) {
        if !matches!(
            item,
            StreamElement::Item(_) | StreamElement::Timestamped(_, _)
        ) {
            log::warn!("Skipping non-item element: {}", item.variant_str());
            return;
        }

        let json = serde_json::to_string(item).expect("Serialization failed");
        let record = BaseRecord::to(self.topic.as_str())
            .payload(&json)
            .key(self.topic_key.as_deref().expect("Topic key not set"));
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

impl<T: ExchangeData> From<&KafkaConfig> for KafkaSinkConnector<T> {
    fn from(value: &KafkaConfig) -> Self {
        KafkaSinkConnector::new(value.brokers.clone(), value.topic.clone())
    }
}
