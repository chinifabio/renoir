use std::{sync::Arc, time::Duration};

use crate::{
    block::{BlockStructure, OperatorKind, OperatorStructure},
    config::DistributedConfig,
    operator::{iteration::IterationStateLock, ExchangeData, Operator, SimpleStartReceiver, Start, StreamElement},
    prelude::Source,
    scheduler::BlockId,
    Replication,
};

use super::{channel::{LayerChannel, LayerChannelExt}, layout_frontier::LayoutFrontier};

#[derive(Debug, Clone)]
enum LayerConnectorSourceInner<T: ExchangeData> {
    Remote(LayerConnector<T>),
    Local(Start<SimpleStartReceiver<T>>),
}

#[derive(Debug, Clone)]
pub struct LayerConnectorSource<Out: ExchangeData> {
    inner: LayerConnectorSourceInner<Out>,
}

impl<T: ExchangeData> std::fmt::Display for LayerConnectorSource<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            LayerConnectorSourceInner::Remote(conn) => write!(f, "{}", conn),
            LayerConnectorSourceInner::Local(start) => write!(f, "{}", start),
        }
    }
}

impl<T: ExchangeData> LayerConnectorSource<T> {
    pub fn new_remote(config: &DistributedConfig) -> Self {
        Self {
            inner: LayerConnectorSourceInner::Remote(LayerConnector::new(config.clone())),
        }
    }

    pub fn new_local(
        previous_block_id: BlockId,
        state_lock: Option<Arc<IterationStateLock>>,
    ) -> Self {
        Self {
            inner: LayerConnectorSourceInner::Local(Start::single(previous_block_id, state_lock)),
        }
    }
}

impl<T: ExchangeData> Operator for LayerConnectorSource<T> {
    type Out = T;

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        match &mut self.inner {
            LayerConnectorSourceInner::Remote(conn) => conn.setup(metadata),
            LayerConnectorSourceInner::Local(start) => start.setup(metadata),
        }
    }

    fn next(&mut self) -> crate::operator::StreamElement<Self::Out> {
        match &mut self.inner {
            LayerConnectorSourceInner::Remote(conn) => conn.next(),
            LayerConnectorSourceInner::Local(start) => start.next(),
        }
    }

    fn structure(&self) -> crate::block::BlockStructure {
        match &self.inner {
            LayerConnectorSourceInner::Remote(conn) => conn.structure(),
            LayerConnectorSourceInner::Local(start) => start.structure(),
        }
    }
}

impl<T: ExchangeData> Source for LayerConnectorSource<T> {
    fn replication(&self) -> Replication {
        match &self.inner {
            LayerConnectorSourceInner::Remote(conn) => conn.replication(),
            LayerConnectorSourceInner::Local(start) => start.replication(),
        }
    }
}

#[derive(Debug)]
struct LayerConnector<T: ExchangeData> {
    config: DistributedConfig,
    frontier: LayoutFrontier,
    channel: Option<LayerChannel<T>>,
}

impl<T: ExchangeData + Clone> Clone for LayerConnector<T> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            frontier: self.frontier.clone(),
            channel: None,
        }
    }
}

impl<T: ExchangeData> std::fmt::Display for LayerConnector<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LayerConnector({})",
            self.channel.as_ref().unwrap_or(&LayerChannel::none())
        )
    }
}

impl<T: ExchangeData> LayerConnector<T> {
    pub fn new(config: DistributedConfig) -> Self {
        Self {
            config,
            frontier: Default::default(),
            channel: None,
        }
    }
}

impl<T: ExchangeData> Operator for LayerConnector<T> {
    type Out = T;

    fn setup(&mut self, _metadata: &mut crate::ExecutionMetadata) {
        self.channel = Some(self.config.get_input_channel());
    }

    fn next(&mut self) -> crate::operator::StreamElement<Self::Out> {
        loop {
            let channel = self.channel.as_mut().expect("Channel not initialized");
            let timeout = Duration::from_secs(self.config.heartbeat_interval);
            let (metadata, item): (super::MessageMetadata, crate::operator::StreamElement<T>) =
                channel
                    .recv_timeout(timeout)
                    .expect("Channel receive failed");

            let item = match item {
                StreamElement::Item(_) | StreamElement::Timestamped(_, _) => Some(item),
                StreamElement::Watermark(_) | StreamElement::FlushBatch | StreamElement::Terminate | StreamElement::FlushAndRestart => self.frontier.update(metadata, &item),
            };

            if let Some(item) = item {
                return item;
            }
        }
    }

    fn structure(&self) -> crate::block::BlockStructure {
        let mut operator = OperatorStructure::new::<Self::Out, _>("LayerConnectorSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<T: ExchangeData> Source for LayerConnector<T> {
    fn replication(&self) -> Replication {
        Replication::Unlimited
    }
}
