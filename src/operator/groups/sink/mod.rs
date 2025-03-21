pub mod kafka;
pub mod redis;

use core::panic;
use std::sync::Arc;

use kafka::KafkaSinkConnector;
use redis::RedisSinkConnector;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::config::{ConnectorTechnology, DistributedConfig};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::CoordUInt;

use self::heartbeat::HeartbeatManager;

use super::{heartbeat, GroupName, GroupStreamElement};

#[derive(Debug)]
pub struct ConnectorSink<T, Op>
where
    T: ExchangeData,
    Op: Operator<Out = T>,
{
    inner: ConnectorSinkTechnology<T>,
    group_name: Option<GroupName>,
    global_id: Option<CoordUInt>,
    prev: Op,
}

impl<T: Clone, Op: Clone> Clone for ConnectorSink<T, Op>
where
    T: ExchangeData,
    Op: Operator<Out = T>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            group_name: self.group_name.clone(),
            global_id: None,
            prev: self.prev.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConnectorSinkTechnology<T: ExchangeData> {
    Kafka(KafkaSinkConnector<T>),
    #[allow(dead_code)]
    Redis(RedisSinkConnector<T>),
    None,
}

pub trait ConnectorSinkStrategy<T: ExchangeData>: Clone + Send {
    fn setup(&mut self, metadata: &mut ExecutionMetadata);
    fn append(&mut self, item: &GroupStreamElement<T>);
    fn technology(&self) -> String;
}

impl<T, Op> ConnectorSink<T, Op>
where
    T: ExchangeData,
    Op: Operator<Out = T>,
{
    pub fn new(prev: Op, config: Arc<DistributedConfig>) -> Self {
        let technology = match config.output_group() {
            ConnectorTechnology::Kafka(kafka_config) => {
                let heartbeat = HeartbeatManager::new(config.clone());
                ConnectorSinkTechnology::Kafka(KafkaSinkConnector::new(
                    kafka_config,
                    heartbeat,
                    config.host_group.clone(),
                ))
            }
            ConnectorTechnology::None => ConnectorSinkTechnology::None,
            e => todo!("Missing implementation for this sink technology: {e:?}"),
        };
        ConnectorSink {
            inner: technology,
            group_name: None,
            global_id: None,
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
        self.group_name = metadata.group_name();
        self.global_id = Some(metadata.global_id);
        self.inner.setup(metadata);
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        let item = GroupStreamElement::wrap(
            self.prev.next(),
            self.group_name.clone(),
            self.global_id.expect("Missing id"),
        );
        self.inner.append(&item);
        item.element
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

    fn append(&mut self, item: &GroupStreamElement<T>) {
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
