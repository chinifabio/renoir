use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use renoir_core::Stream;
use renoir_core::block::structure::{BlockStructure, OperatorKind, OperatorStructure};
use renoir_core::operator::{Operator, StreamElement};
use renoir_core::scheduler::ExecutionMetadata;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct KafkaSink<Op>
where
    Op: Operator,
{
    prev: Op,
    #[derivative(Debug = "ignore")]
    producer: FutureProducer,
    topic: Arc<String>,
    rt: Option<tokio::runtime::Handle>,
}

impl<Op> KafkaSink<Op>
where
    Op: Operator,
{
    pub(crate) fn new(prev: Op, producer: FutureProducer, topic: String) -> Self {
        Self {
            prev,
            producer,
            topic: Arc::new(topic),
            rt: None, // tokio::runtime::Handle::current(),
        }
    }
}

impl<Op> Display for KafkaSink<Op>
where
    Op: Operator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> KafkaSink", self.prev)
    }
}

impl<Op> Operator for KafkaSink<Op>
where
    Op: Operator,
    Op::Out: AsRef<[u8]> + 'static,
{
    type Out = ();

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        if self.rt.is_none() {
            self.rt = Some(tokio::runtime::Handle::current());
        }
    }

    fn next(&mut self) -> StreamElement<()> {
        loop {
            match self.prev.next() {
                StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                    let producer = self.producer.clone();
                    let topic = self.topic.clone();

                    self.rt.as_ref().unwrap().spawn(async move {
                        let record = FutureRecord::to(&topic).key(&[]).payload(t.as_ref());

                        producer
                            .send(record, Timeout::After(Duration::from_secs(10)))
                            .await
                            .expect("kafka producer fail");
                    });
                }
                StreamElement::Watermark(w) => return StreamElement::Watermark(w),
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Op::Out, _>("KafkaSinkSink");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}

pub trait KafkaSinkExt<Op>
where
    Op: Operator,
{
    /// # WARNING: KAFKA API IS EXPERIMENTAL
    fn write_kafka(self, producer_config: ClientConfig, topic: &str);
}

impl<Op> KafkaSinkExt<Op> for Stream<Op>
where
    Op: Operator<Out: AsRef<[u8]>> + 'static,
{
    /// # WARNING: KAFKA API IS EXPERIMENTAL
    fn write_kafka(self, producer_config: ClientConfig, topic: &str) {
        let producer = producer_config
            .create::<FutureProducer>()
            .expect("failed to create kafka producer");
        self.add_operator(|prev| KafkaSink::new(prev, producer, topic.to_owned()))
            .finalize_block();
    }
}
