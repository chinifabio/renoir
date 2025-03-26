use std::hash::{DefaultHasher, Hash, Hasher};

use crate::{
    block::{OperatorKind, OperatorStructure},
    config::DistributedConfig,
    operator::{ExchangeData, Operator, StreamElement},
};

use super::{
    channel::{LayerChannel, LayerChannelExt},
    MessageMetadata,
};

#[derive(Debug)]
pub(crate) struct LayerConnectorSink<T, Op>
where
    T: ExchangeData,
    Op: Operator<Out = T>,
{
    config: DistributedConfig,
    channel: Option<LayerChannel<T>>,
    metadata: Option<MessageMetadata>,
    prev: Op,
}

impl<T: Clone, Op: Clone> Clone for LayerConnectorSink<T, Op>
where
    T: ExchangeData,
    Op: Operator<Out = T>,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            channel: None,
            metadata: self.metadata.clone(),
            prev: self.prev.clone(),
        }
    }
}

impl<T, Op> std::fmt::Display for LayerConnectorSink<T, Op>
where
    T: ExchangeData,
    Op: Operator<Out = T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> LayerConnectorSink({})",
            self.prev,
            self.channel.as_ref().unwrap_or(&LayerChannel::none())
        )
    }
}

impl<T, Op> LayerConnectorSink<T, Op>
where
    T: ExchangeData,
    Op: Operator<Out = T>,
{
    pub fn new(prev: Op, config: DistributedConfig) -> Self {
        Self {
            config,
            channel: None,
            metadata: None,
            prev,
        }
    }
}

impl<T, Op> Operator for LayerConnectorSink<T, Op>
where
    T: ExchangeData,
    Op: Operator<Out = T>,
{
    type Out = T;

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.prev.setup(metadata);

        // Set the metadata for this replica
        let mut hasher = DefaultHasher::new();
        self.config.hash(&mut hasher);
        self.metadata = Some(MessageMetadata::new(
            self.config.layer.clone(),
            hasher.finish(),
            metadata.parallelism,
        ));

        // Set the channel for this replica
        self.channel = Some(self.config.get_output_channel());

        // TODO: add cancel token and start a thread for the heartbeat -> broadcast o send?
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        let item = self.prev.next();

        let channel = self.channel.as_mut().expect("Channel not set");
        let metadata = self.metadata.as_ref().expect("Metadata not set");
        match &item {
            StreamElement::Item(_) | StreamElement::Timestamped(_, _) => {
                channel.send(&metadata, &item);
            }
            StreamElement::Watermark(_) => {
                channel.broadcast(&metadata, &item);
            }

            StreamElement::FlushBatch
            | StreamElement::Terminate
            | StreamElement::FlushAndRestart => {
                // self.channel.wait(); TODO capire se serve wait o lo faccio a livello di tecnologia
                channel.broadcast(&metadata, &item);
            }
        };
        item
    }

    fn structure(&self) -> crate::block::BlockStructure {
        let mut operator = OperatorStructure::new::<Op::Out, _>("LayerConnectorSink");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}
