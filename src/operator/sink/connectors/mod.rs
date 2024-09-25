pub mod kafka;
pub mod redis;

use kafka::KafkaSinkConnector;
use redis::RedisSinkConnector;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

pub trait ConnectorSinkStrategy<T: ExchangeData>: Clone + Send {
    fn setup(&mut self, metadata: &mut ExecutionMetadata);
    fn append(&mut self, item: &StreamElement<T>);
    fn technology(&self) -> String;
}

#[derive(Debug, Clone)]
pub enum ConnectorSinkTechnology<T: ExchangeData> {
    Kafka(KafkaSinkConnector<T>),
    Redis(RedisSinkConnector<T>),
}

impl<T> ConnectorSinkStrategy<T> for ConnectorSinkTechnology<T>
where
    T: ExchangeData,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        match self {
            ConnectorSinkTechnology::Kafka(connector) => connector.setup(metadata),
            ConnectorSinkTechnology::Redis(connector) => connector.setup(metadata),
        }
    }

    fn append(&mut self, item: &StreamElement<T>) {
        match self {
            ConnectorSinkTechnology::Kafka(connector) => connector.append(item),
            ConnectorSinkTechnology::Redis(connector) => connector.append(item),
        }
    }

    fn technology(&self) -> String {
        match self {
            ConnectorSinkTechnology::Kafka(_) => "Kafka".to_string(),
            ConnectorSinkTechnology::Redis(_) => "Redis".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectorSink<T, S, Op>
where
    T: ExchangeData,
    S: ConnectorSinkStrategy<T>,
    Op: Operator<Out = T>,
{
    strategy: S,
    _phantom: std::marker::PhantomData<T>,
    prev: Op,
}

impl<T, S, Op> ConnectorSink<T, S, Op>
where
    T: ExchangeData,
    S: ConnectorSinkStrategy<T>,
    Op: Operator<Out = T>,
{
    pub fn new(strategy: S, prev: Op) -> Self {
        Self {
            strategy,
            _phantom: std::marker::PhantomData,
            prev,
        }
    }
}

impl<T, S, Op> std::fmt::Display for ConnectorSink<T, S, Op>
where
    T: ExchangeData,
    S: ConnectorSinkStrategy<T>,
    Op: Operator<Out = T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> {}Sink", self.prev, self.strategy.technology())
    }
}

impl<T, S, Op> Operator for ConnectorSink<T, S, Op>
where
    T: ExchangeData,
    S: ConnectorSinkStrategy<T>,
    Op: Operator<Out = T>,
{
    type Out = ();

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.strategy.setup(metadata);
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        let item = self.prev.next();
        self.strategy.append(&item);
        item.map(|_| ())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Self::Out, _>("ConnectorSink");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}
