use kafka::KafkaChannel;

use crate::{
    config::{ChannelConfig, DistributedConfig, KafkaConfig},
    operator::{ExchangeData, StreamElement},
};

use super::{MessageMetadata, RenoirMessage};

mod kafka;

#[derive(Debug, Default)]
pub(crate) enum LayerChannelInner<T: ExchangeData> {
    Kafka(KafkaChannel<T>),
    #[default]
    None,
}

#[derive(Debug, Default)]
pub(crate) struct LayerChannel<T: ExchangeData> {
    inner: LayerChannelInner<T>,
}

impl DistributedConfig {
    pub(crate) fn get_input_channel<T: ExchangeData>(&self) -> LayerChannel<T> {
        self.group_input
            .as_ref()
            .map(|channel_config| match channel_config {
                ChannelConfig::Kafka(config) => {
                    LayerChannel::new_kafka_consumer(config.clone(), self.layer.clone())
                }
                ChannelConfig::None => LayerChannel::none(),
            })
            .unwrap_or(LayerChannel::none())
    }

    pub(crate) fn get_output_channel<T: ExchangeData>(&self) -> LayerChannel<T> {
        self.group_output
            .as_ref()
            .map(|channel_config| match channel_config {
                ChannelConfig::Kafka(config) => {
                    LayerChannel::new_kafka_producer(config.clone(), self.layer.clone())
                }
                ChannelConfig::None => LayerChannel::none(),
            })
            .unwrap_or(LayerChannel::none())
    }
}

impl<T: ExchangeData> std::fmt::Display for LayerChannel<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            LayerChannelInner::Kafka(_) => write!(f, "Kafka"),
            LayerChannelInner::None => write!(f, "None"),
        }
    }
}

pub(crate) trait LayerChannelExt<T: ExchangeData> {
    fn send(&mut self, metadata: &MessageMetadata, item: &StreamElement<T>);
    fn broadcast(&mut self, metadata: &MessageMetadata, item: &StreamElement<T>);
    #[allow(dead_code)]
    fn recv(&mut self) -> Option<RenoirMessage<T>>;
    fn recv_timeout(&mut self, timeout: std::time::Duration) -> Option<RenoirMessage<T>>;
}

impl<T: ExchangeData> LayerChannel<T> {
    pub fn none() -> Self {
        Self {
            inner: LayerChannelInner::None,
        }
    }

    pub fn new_kafka_producer(config: KafkaConfig, layer: String) -> Self {
        Self {
            inner: LayerChannelInner::Kafka(KafkaChannel::new_producer(config, layer)),
        }
    }

    pub fn new_kafka_consumer(config: KafkaConfig, layer: String) -> Self {
        Self {
            inner: LayerChannelInner::Kafka(KafkaChannel::new_consumer(config, layer)),
        }
    }
}

impl<T: ExchangeData> LayerChannelExt<T> for LayerChannel<T> {
    fn send(&mut self, metadata: &MessageMetadata, item: &StreamElement<T>) {
        match &mut self.inner {
            LayerChannelInner::Kafka(channel) => channel.send(metadata, item),
            LayerChannelInner::None => {}
        }
    }

    fn broadcast(&mut self, metadata: &MessageMetadata, item: &StreamElement<T>) {
        match &mut self.inner {
            LayerChannelInner::Kafka(channel) => channel.broadcast(metadata, item),
            LayerChannelInner::None => {}
        }
    }

    fn recv(&mut self) -> Option<RenoirMessage<T>> {
        match &mut self.inner {
            LayerChannelInner::Kafka(channel) => channel.recv(),
            LayerChannelInner::None => None,
        }
    }

    fn recv_timeout(&mut self, timeout: std::time::Duration) -> Option<RenoirMessage<T>> {
        match &mut self.inner {
            LayerChannelInner::Kafka(channel) => channel.recv_timeout(timeout),
            LayerChannelInner::None => None,
        }
    }
}
