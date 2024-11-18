pub mod kafka;
pub mod redis;

use std::fmt::Display;
use std::sync::Arc;

use kafka::KafkaSourceConnector;
use redis::RedisSourceConnector;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::operator::iteration::IterationStateLock;
use crate::operator::source::Source;
use crate::operator::{ExchangeData, Operator, SimpleStartReceiver, Start, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::Stream;

#[derive(Debug)]
pub struct ConnectorSource<T: ExchangeData> {
    inner: ConnectorSourceInner<T>,
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum ConnectorSourceInner<T: ExchangeData> {
    Local(Start<SimpleStartReceiver<T>>),
    Remote(ConnectorSourceTechnology<T>),
}

#[derive(Debug, Clone)]
pub enum ConnectorSourceTechnology<T: ExchangeData> {
    Kafka(KafkaSourceConnector<T>),
    Redis(RedisSourceConnector<T>),
    None,
}

pub trait ConnectorSourceStrategy<T: ExchangeData>: Clone + Send {
    fn replication(&self) -> Replication;
    fn setup(&mut self, metadata: &mut ExecutionMetadata);
    fn next(&mut self) -> StreamElement<T>;
    fn technology(&self) -> String;
}

impl<T: ExchangeData + Clone> Clone for ConnectorSource<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: ExchangeData> Display for ConnectorSource<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            ConnectorSourceInner::Local(_) => write!(f, "GroupStart"),
            ConnectorSourceInner::Remote(connector_source_technology) => write!(
                f,
                "GroupStart({})",
                connector_source_technology.technology()
            ),
        }
    }
}

impl<T: ExchangeData> Source for ConnectorSource<T> {
    fn replication(&self) -> Replication {
        match &self.inner {
            ConnectorSourceInner::Local(start) => start.replication(),
            ConnectorSourceInner::Remote(tech) => tech.replication(),
        }
    }
}

impl<T: ExchangeData> Operator for ConnectorSource<T> {
    type Out = T;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        match &mut self.inner {
            ConnectorSourceInner::Local(start) => start.setup(metadata),
            ConnectorSourceInner::Remote(tech) => tech.setup(metadata),
        }
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        match &mut self.inner {
            ConnectorSourceInner::Local(start) => start.next(),
            ConnectorSourceInner::Remote(tech) => tech.next(),
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Self::Out, _>("ConnectorSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<T: ExchangeData> ConnectorSourceStrategy<T> for ConnectorSourceTechnology<T> {
    fn replication(&self) -> Replication {
        match self {
            ConnectorSourceTechnology::Kafka(connector) => connector.replication(),
            ConnectorSourceTechnology::Redis(connector) => connector.replication(),
            ConnectorSourceTechnology::None => Replication::default(),
        }
    }

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        match self {
            ConnectorSourceTechnology::Kafka(connector) => connector.setup(metadata),
            ConnectorSourceTechnology::Redis(connector) => connector.setup(metadata),
            ConnectorSourceTechnology::None => {}
        }
    }

    fn next(&mut self) -> StreamElement<T> {
        match self {
            ConnectorSourceTechnology::Kafka(connector) => connector.next(),
            ConnectorSourceTechnology::Redis(connector) => connector.next(),
            ConnectorSourceTechnology::None => unreachable!(),
        }
    }

    fn technology(&self) -> String {
        match self {
            ConnectorSourceTechnology::Kafka(_) => "Kafka".to_string(),
            ConnectorSourceTechnology::Redis(_) => "Redis".to_string(),
            ConnectorSourceTechnology::None => "None".to_string(),
        }
    }
}

impl<T: ExchangeData> ConnectorSource<T> {
    pub(crate) fn new_local(
        previous_block_id: u64,
        state_lock: Option<Arc<IterationStateLock>>,
    ) -> Self {
        ConnectorSource {
            inner: ConnectorSourceInner::Local(Start::single(previous_block_id, state_lock)),
        }
    }

    pub(crate) fn new_remote(technology: ConnectorSourceTechnology<T>) -> Self {
        ConnectorSource {
            inner: ConnectorSourceInner::Remote(technology),
        }
    }
}

impl crate::StreamContext {
    pub fn stream_connector<T: ExchangeData>(
        &self,
        connector: ConnectorSourceTechnology<T>,
    ) -> Stream<impl Operator<Out = T>> {
        let source = ConnectorSource::new_remote(connector);
        self.stream(source)
    }
}
