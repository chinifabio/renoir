#[cfg(feature = "rdkafka")]
use kafka::KafkaChannel;

#[cfg(feature = "rdkafka")]
use crate::config::KafkaConfig;

use crate::{
    config::{ChannelConfig, DistributedConfig},
    operator::{ExchangeData, StreamElement},
};

use super::{MessageMetadata, RenoirMessage};

#[cfg(feature = "rdkafka")]
mod kafka;

#[derive(Debug)]
pub(crate) enum LayerChannelInner<T: ExchangeData> {
    #[cfg(feature = "rdkafka")]
    Kafka(KafkaChannel<T>),
    None(std::marker::PhantomData<T>),
}

impl<T: ExchangeData> Default for LayerChannelInner<T> {
    fn default() -> Self {
        LayerChannelInner::None(std::marker::PhantomData)
    }
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
                #[cfg(feature = "rdkafka")]
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
                #[cfg(feature = "rdkafka")]
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
            #[cfg(feature = "rdkafka")]
            LayerChannelInner::Kafka(_) => write!(f, "Kafka"),
            LayerChannelInner::None(_) => write!(f, "None"),
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
            inner: LayerChannelInner::None(std::marker::PhantomData),
        }
    }

    #[cfg(feature = "rdkafka")]
    pub fn new_kafka_producer(config: KafkaConfig, layer: String) -> Self {
        Self {
            inner: LayerChannelInner::Kafka(KafkaChannel::new_producer(config, layer)),
        }
    }

    #[cfg(feature = "rdkafka")]
    pub fn new_kafka_consumer(config: KafkaConfig, layer: String) -> Self {
        Self {
            inner: LayerChannelInner::Kafka(KafkaChannel::new_consumer(config, layer)),
        }
    }
}

impl<T: ExchangeData> LayerChannelExt<T> for LayerChannel<T> {
    fn send(&mut self, metadata: &MessageMetadata, item: &StreamElement<T>) {
        match &mut self.inner {
            #[cfg(feature = "rdkafka")]
            LayerChannelInner::Kafka(channel) => channel.send(metadata, item),
            LayerChannelInner::None(_) => {
                let _ = metadata;
                let _ = item;
            }
        }
    }

    fn broadcast(&mut self, metadata: &MessageMetadata, item: &StreamElement<T>) {
        match &mut self.inner {
            #[cfg(feature = "rdkafka")]
            LayerChannelInner::Kafka(channel) => channel.broadcast(metadata, item),
            LayerChannelInner::None(_) => {
                let _ = metadata;
                let _ = item;
            }
        }
    }

    fn recv(&mut self) -> Option<RenoirMessage<T>> {
        match &mut self.inner {
            #[cfg(feature = "rdkafka")]
            LayerChannelInner::Kafka(channel) => channel.recv(),
            LayerChannelInner::None(_) => None,
        }
    }

    fn recv_timeout(&mut self, timeout: std::time::Duration) -> Option<RenoirMessage<T>> {
        match &mut self.inner {
            #[cfg(feature = "rdkafka")]
            LayerChannelInner::Kafka(channel) => channel.recv_timeout(timeout),
            LayerChannelInner::None(_) => {
                let _ = timeout;
                None
            }
        }
    }
}
