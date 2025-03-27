mod channel;
mod layout_frontier;
mod sink;
mod source;

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
                let block = block
                    .add_operator(|prev| LayerConnectorSink::new(prev, distributed_config.clone())); // TODO cambiare il clone con Arc o anche Rc
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct MessageMetadata {
    /// Layer name
    layer: String,
    /// Remote Config Hash. this is to identify the replica of the layer
    rch: u64,
    /// Parallelism level, used to await for all the broadcasts messages
    parallelism: u64,
}

impl std::fmt::Debug for MessageMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageMetadata")
            .field("layer", &self.layer)
            .field("rch", &format_args!("{:x}", self.rch))
            .field("parallelism", &self.parallelism)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RenoirMessage<T> {
    metadata: MessageMetadata,
    element: Option<StreamElement<T>>,
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

// TODO fare il tipo Message come (gruppo metadata, optional di element)
