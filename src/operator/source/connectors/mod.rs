pub mod kafka;
pub mod redis;

use std::fmt::Display;

use kafka::KafkaSourceConnector;
use redis::RedisSourceConnector;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::operator::source::Source;
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::Stream;

pub trait ConnectorSourceStrategy<T: ExchangeData>: Clone + Send {
    fn replication(&self) -> Replication;
    fn setup(&mut self, metadata: &mut ExecutionMetadata);
    fn next(&mut self) -> StreamElement<T>;
    fn technology(&self) -> String;
}

#[derive(Debug, Clone)]
pub enum ConnectorSourceTechnology<T: ExchangeData> {
    Kafka(KafkaSourceConnector<T>),
    Redis(RedisSourceConnector<T>),
}

impl<T: ExchangeData> ConnectorSourceStrategy<T> for ConnectorSourceTechnology<T> {
    fn replication(&self) -> Replication {
        match self {
            ConnectorSourceTechnology::Kafka(connector) => connector.replication(),
            ConnectorSourceTechnology::Redis(connector) => connector.replication(),
        }
    }

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        match self {
            ConnectorSourceTechnology::Kafka(connector) => connector.setup(metadata),
            ConnectorSourceTechnology::Redis(connector) => connector.setup(metadata),
        }
    }

    fn next(&mut self) -> StreamElement<T> {
        match self {
            ConnectorSourceTechnology::Kafka(connector) => connector.next(),
            ConnectorSourceTechnology::Redis(connector) => connector.next(),
        }
    }

    fn technology(&self) -> String {
        match self {
            ConnectorSourceTechnology::Kafka(_) => "Kafka".to_string(),
            ConnectorSourceTechnology::Redis(_) => "Redis".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectorSource<T, S>
where
    T: ExchangeData,
    S: ConnectorSourceStrategy<T>,
{
    strategy: S,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, S> ConnectorSource<T, S>
where
    T: ExchangeData,
    S: ConnectorSourceStrategy<T>,
{
    pub fn new(strategy: S) -> Self {
        Self {
            strategy,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T, S> Display for ConnectorSource<T, S>
where
    T: ExchangeData,
    S: ConnectorSourceStrategy<T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}Source<{}>",
            self.strategy.technology(),
            std::any::type_name::<T>()
        )
    }
}

impl<T, S> Source for ConnectorSource<T, S>
where
    T: ExchangeData,
    S: ConnectorSourceStrategy<T>,
{
    fn replication(&self) -> Replication {
        self.strategy.replication()
    }
}

impl<T, S> Operator for ConnectorSource<T, S>
where
    T: ExchangeData,
    S: ConnectorSourceStrategy<T>,
{
    type Out = T;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.strategy.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        self.strategy.next()
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Self::Out, _>("ConnectorSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl crate::StreamContext {
    pub fn stream_connector<T, S>(&self, connector: S) -> Stream<impl Operator<Out = T>>
    where
        T: ExchangeData,
        S: ConnectorSourceStrategy<T> + 'static,
    {
        let source = ConnectorSource::new(connector);
        self.stream(source)
    }
}
