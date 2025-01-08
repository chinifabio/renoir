use std::{collections::HashMap, sync::Arc, time::Duration};

use parking_lot::lock_api::Mutex;
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    util::Timeout,
    ClientConfig, Message,
};

use crate::operator::{
    groups::{heartbeat::HeartbeatManager, GroupMap},
    ExchangeData, StreamElement,
};

use super::{heartbeat::WatermarkManager, ConnectorSourceStrategy, GroupStreamElement};

pub struct KafkaSourceConnector<T: ExchangeData> {
    hosts: Vec<String>,
    consumer: Option<BaseConsumer>,
    topic: String,
    timeout: Timeout,
    _phantom: std::marker::PhantomData<T>,

    groups: GroupMap,
    heartbeat: HeartbeatManager,
    watermarks: WatermarkManager,
}

impl<T: ExchangeData + Clone> Clone for KafkaSourceConnector<T> {
    fn clone(&self) -> Self {
        Self {
            hosts: self.hosts.clone(),
            consumer: None,
            topic: self.topic.clone(),
            timeout: self.timeout,
            _phantom: self._phantom,
            groups: self.groups.clone(),
            heartbeat: self.heartbeat.clone(),
            watermarks: self.watermarks.clone(),
        }
    }
}

impl<T: ExchangeData + std::fmt::Debug> std::fmt::Debug for KafkaSourceConnector<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSourceConnector")
            .field("hosts", &self.hosts)
            .field("topic", &self.topic)
            .field("timeout", &self.timeout)
            .field("_phantom", &self._phantom)
            .field("groups", &self.groups)
            .field("heartbeat", &self.heartbeat)
            .field("watermarks", &self.watermarks)
            .finish()
    }
}

impl<T: ExchangeData> KafkaSourceConnector<T> {
    pub fn new(
        hosts: Vec<String>,
        topic: impl Into<String>,
        timeout: Option<u64>,
        heartbeat: HeartbeatManager,
    ) -> Self {
        let timeout = match timeout {
            Some(timeout) => Timeout::After(Duration::from_secs(timeout)),
            None => Timeout::Never,
        };
        let groups = Arc::new(Mutex::new(HashMap::new()));

        Self {
            hosts,
            consumer: None,
            topic: topic.into(),
            _phantom: std::marker::PhantomData,
            timeout,
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

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        let consumer_group = format!(
            "renoir-{}",
            metadata.group_name().as_deref().unwrap_or("default")
        );

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

        // I'm using replica_id since its different per host and I need a different Arc per host
        if metadata.coord.replica_id == 0 {
            self.heartbeat
                .start_receiver(self.groups.clone(), metadata.global_id);
        }
    }

    // TODO: aggiungere i flush batch, ecc..
    fn next(&mut self) -> GroupStreamElement<T> {
        let consumer = self
            .consumer
            .as_mut()
            .expect("Kafka consumer not configured");

        loop {
            let item = match consumer.poll(self.timeout) {
                Some(Ok(message)) => {
                    let payload = message.payload().unwrap();
                    let item: GroupStreamElement<T> =
                        serde_json::from_slice(payload).expect("Failed to parse the element");
                    match item.element {
                        StreamElement::Item(_) | StreamElement::Timestamped(_, _) => Some(item),
                        StreamElement::Watermark(watermark_time) => self
                            .watermarks
                            .update(
                                item.group
                                    .as_deref()
                                    .expect("Missing group name in item read from group connection")
                                    .to_string(),
                                watermark_time,
                                item.id,
                            )
                            .map(|min_time| {
                                GroupStreamElement::wrap(
                                    StreamElement::Watermark(min_time),
                                    item.group,
                                    item.id,
                                )
                            }),
                        e => unreachable!(
                            "Group Source received {}, but it's not allowed.",
                            e.variant_str()
                        ),
                    }
                }
                Some(Err(e)) => {
                    panic!(
                        "Failed to poll message from Kafka on topic {}: {:?}",
                        self.topic, e
                    );
                }
                None => {
                    log::warn!("Ignoring empty message from Kafka");
                    None
                }
            };

            if let Some(item) = item {
                return item;
            }
        }
    }

    fn technology(&self) -> String {
        "Kafka".to_string()
    }
}
