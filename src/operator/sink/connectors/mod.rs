pub mod kafka;
pub mod redis;

use core::panic;

use kafka::KafkaSinkConnector;
use redis::RedisSinkConnector;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

#[derive(Debug, Clone)]
pub struct ConnectorSink<T, Op>
where
    T: ExchangeData,
    Op: Operator<Out = T>,
{
    inner: ConnectorSinkTechnology<T>,
    prev: Op,
}

#[derive(Debug, Clone)]
pub enum ConnectorSinkTechnology<T: ExchangeData> {
    Kafka(KafkaSinkConnector<T>),
    Redis(RedisSinkConnector<T>),
    None,
}

pub trait ConnectorSinkStrategy<T: ExchangeData>: Clone + Send {
    fn setup(&mut self, metadata: &mut ExecutionMetadata);
    fn append(&mut self, item: &StreamElement<T>);
    fn technology(&self) -> String;
}

impl<T, Op> ConnectorSink<T, Op>
where
    T: ExchangeData,
    Op: Operator<Out = T>,
{
    pub fn new(prev: Op, technology: ConnectorSinkTechnology<T>) -> Self {
        ConnectorSink {
            inner: technology,
            prev,
        }
    }
}

impl<T, Op> std::fmt::Display for ConnectorSink<T, Op>
where
    T: ExchangeData,
    Op: Operator<Out = T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> GroupEnd({})", self.prev, self.inner.technology())
    }
}

impl<T, Op> Operator for ConnectorSink<T, Op>
where
    T: ExchangeData,
    Op: Operator<Out = T>,
{
    type Out = T;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.inner.setup(metadata);
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        let item = self.prev.next();
        self.inner.append(&item);
        item
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Self::Out, _>("ConnectorSink");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}

impl<T> ConnectorSinkStrategy<T> for ConnectorSinkTechnology<T>
where
    T: ExchangeData,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        match self {
            ConnectorSinkTechnology::Kafka(connector) => connector.setup(metadata),
            ConnectorSinkTechnology::Redis(connector) => connector.setup(metadata),
            ConnectorSinkTechnology::None => panic!("No technology specified for the sink"),
        }
    }

    fn append(&mut self, item: &StreamElement<T>) {
        match self {
            ConnectorSinkTechnology::Kafka(connector) => connector.append(item),
            ConnectorSinkTechnology::Redis(connector) => connector.append(item),
            ConnectorSinkTechnology::None => panic!("No technology specified for the sink"),
        }
    }

    fn technology(&self) -> String {
        match self {
            ConnectorSinkTechnology::Kafka(_) => "Kafka".to_string(),
            ConnectorSinkTechnology::Redis(_) => "Redis".to_string(),
            ConnectorSinkTechnology::None => "None".to_string(),
        }
    }
}
