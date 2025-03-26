mod channel;
mod sink;
mod source;
mod layout_frontier;

use serde::{Deserialize, Serialize};
use sink::LayerConnectorSink;
use source::LayerConnectorSource;

use crate::{
    block::NextStrategy,
    operator::{end::End, ExchangeData, Operator, StreamElement},
    RuntimeConfig, Stream,
};

impl<I, Op> Stream<Op>
where
    I: ExchangeData,
    Op: Operator<Out = I> + 'static,
{
    pub fn change_layer(self, name: impl Into<String>) -> Stream<impl Operator<Out = Op::Out>> {
        let Stream { block, ctx } = self;
        let mut lock = ctx.lock();
        let batch_mode = block.batch_mode;
        let iteration_ctx = block.iteration_ctx.clone();

        let prev_id = match &*lock.config {
            RuntimeConfig::Distributed {
                distributed_config, ..
            } => {
                let block = block.add_operator(|prev| {
                    LayerConnectorSink::new(prev, distributed_config.clone())
                }); // TODO cambiare il clone con Arc o anche Rc
                lock.close_block(block)
            }
            _ => {
                let block = block.add_operator(|prev| {
                    End::new(prev, NextStrategy::random(), Default::default())
                });
                lock.close_block(block)
            }
        };

        let new_block = match &*lock.config {
            RuntimeConfig::Distributed {
                distributed_config, ..
            } => {
                let source = LayerConnectorSource::new_remote(distributed_config);
                lock.update_layer(name);
                lock.new_block(source, Default::default(), Default::default())
            }
            _ => {
                let source =
                    LayerConnectorSource::new_local(prev_id, iteration_ctx.last().cloned());
                let new_block = lock.new_block(source, batch_mode, iteration_ctx);
                lock.connect_blocks::<Op::Out>(prev_id, new_block.id);
                new_block
            }
        };

        drop(lock);
        Stream {
            block: new_block,
            ctx,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum MessageContent<T> {
    Element(StreamElement<T>),
    Heartbeat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MessageMetadata {
    /// Layer name
    layer: String,
    /// Remote Config Hash. this is to identify the replica of the layer
    rch: u64,
    /// Parallelism level, used to await for all the broadcasts messages
    parallelism: u64,
}

#[derive(Debug, Clone)]
struct Message<T: ExchangeData> {
    metadata: MessageMetadata,
    content: MessageContent<T>,
}

impl MessageMetadata {
    fn new(layer: String, rch: u64, parallelism: u64) -> Self {
        Self {
            layer,
            rch,
            parallelism,
        }
    }
}

impl<T: ExchangeData> Message<T> {
    fn new_element(metadata: MessageMetadata, element: StreamElement<T>) -> Self {
        Self {
            metadata,
            content: MessageContent::Element(element),
        }
    }

    fn new_heartbeat(metadata: MessageMetadata) -> Self {
        Self {
            metadata,
            content: MessageContent::Heartbeat,
        }
    }
}
