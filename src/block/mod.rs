use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub use batcher::BatchMode;
pub(crate) use batcher::Batcher;
pub(crate) use graph_generator::*;
pub(crate) use next_strategy::*;
pub(crate) use structure::*;

use crate::operator::iteration::IterationStateLock;
use crate::operator::Operator;
use crate::scheduler::BlockId;
use crate::CoordUInt;

mod batcher;
mod graph_generator;
mod next_strategy;
pub mod structure;

/// A chain of operators that will be run inside the same host. The block takes as input elements of
/// type `In` and produces elements of type `Out`.
///
/// The type `In` is used to make sure the blocks are connected following the correct type.
///
/// `OperatorChain` is the type of the chain of operators inside the block. It must be an operator
/// that yields values of type `Out`.
#[derive(Debug)]
pub(crate) struct Block<OperatorChain>
where
    OperatorChain: Operator,
{
    /// The identifier of the block inside the environment.
    pub(crate) id: BlockId,
    /// The current chain of operators.
    pub(crate) operators: OperatorChain,
    /// The batch mode of this block.
    pub(crate) batch_mode: BatchMode,
    /// This block may be inside a number of iteration loops, this stack keeps track of the state
    /// lock for each of them.
    pub(crate) iteration_ctx: Vec<Arc<IterationStateLock>>,
    /// Whether this block has `NextStrategy::OnlyOne`.
    pub(crate) is_only_one_strategy: bool,
    /// The set of requirements that the block imposes on the scheduler.
    pub(crate) scheduling: Scheduling,
    /// The layer of a block is used to determine on which host the block will be executed.
    pub(crate) layer: Option<String>,
}

impl<OperatorChain> Clone for Block<OperatorChain>
where
    OperatorChain: Operator,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            operators: self.operators.clone(),
            batch_mode: self.batch_mode,
            iteration_ctx: self.iteration_ctx.clone(),
            is_only_one_strategy: self.is_only_one_strategy,
            scheduling: self.scheduling.clone(),
            layer: self.layer.clone(),
        }
    }
}

impl<OperatorChain> Block<OperatorChain>
where
    OperatorChain: Operator,
{
    /// Add an operator to the end of the block
    pub fn add_operator<Op2, GetOp>(self, get_operator: GetOp) -> Block<Op2>
    where
        Op2: Operator,
        GetOp: FnOnce(OperatorChain) -> Op2,
    {
        Block {
            id: self.id,
            operators: get_operator(self.operators),
            batch_mode: self.batch_mode,
            iteration_ctx: self.iteration_ctx,
            is_only_one_strategy: false,
            scheduling: self.scheduling,
            layer: self.layer.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Scheduling {
    /// If some of the operators inside the chain require a limit on the parallelism of this node,
    /// it is stored here. `None` means that the scheduler is allowed to spawn as many copies of
    /// this block as it likes.
    ///
    /// The value specified is only an upper bound, the scheduler is allowed to spawn less blocks,
    pub(crate) replication: Replication,
}

/// Replication factor for a block
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum Replication {
    /// The number of replicas is unlimited and will be determined by the launch configuration.
    #[default]
    Unlimited,
    /// The number of replicas is limited to a fixed number.
    Limited(CoordUInt),
    /// The number of replicas is limited to one per host.
    Host,
    /// The number of replicas is limited to one across all the hosts.
    One,
}

impl Replication {
    pub fn new_unlimited() -> Self {
        Self::Unlimited
    }

    pub fn new_limited(size: CoordUInt) -> Self {
        assert!(size > 0, "Replication limit must be greater than zero!");
        Self::Limited(size)
    }

    pub fn new_host() -> Self {
        Self::Host
    }

    pub fn new_one() -> Self {
        Self::One
    }

    pub fn is_unlimited(&self) -> bool {
        matches!(self, Replication::Unlimited)
    }
    pub fn intersect(&self, rhs: Self) -> Self {
        match (*self, rhs) {
            (Replication::One, _) | (_, Replication::One) => Replication::One,
            (Replication::Host, _) | (_, Replication::Host) => Replication::Host,
            (Replication::Limited(n), Replication::Limited(m)) => Replication::Limited(n.min(m)),
            (Replication::Limited(n), _) | (_, Replication::Limited(n)) => Replication::Limited(n),
            (Replication::Unlimited, Replication::Unlimited) => Replication::Unlimited,
        }
    }

    pub(crate) fn clamp(&self, n: CoordUInt) -> CoordUInt {
        match self {
            Replication::Unlimited => n,
            Replication::Limited(q) => n.min(*q),
            Replication::Host => 1,
            Replication::One => 1,
        }
    }
}

impl<OperatorChain> Block<OperatorChain>
where
    OperatorChain: Operator,
{
    pub fn new(
        id: BlockId,
        operators: OperatorChain,
        batch_mode: BatchMode,
        iteration_ctx: Vec<Arc<IterationStateLock>>,
        scheduling: Scheduling,
    ) -> Self {
        Self {
            id,
            operators,
            batch_mode,
            iteration_ctx,
            is_only_one_strategy: false,
            scheduling,
            layer: None,
        }
    }

    /// Set the layer of this block.
    /// 
    /// The layer is used to determine on which host the block will be executed.
    pub fn set_layer(&mut self, layer: impl Into<String>) {
        self.layer = Some(layer.into());
    }

    /// Obtain a vector of opaque items representing the stack of iterations.
    ///
    /// An empty vector is returned when the block is outside any iterations, more than one element
    /// if it's inside nested iterations.
    pub(crate) fn iteration_ctx(&self) -> Vec<*const ()> {
        self.iteration_ctx
            .iter()
            .map(|s| Arc::as_ptr(s) as *const ())
            .collect()
    }
}

impl<OperatorChain> Display for Block<OperatorChain>
where
    OperatorChain: Operator,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.operators)
    }
}

impl Scheduling {
    /// Limit the maximum parallelism of this block.
    pub(crate) fn replication(&mut self, replication: Replication) {
        self.replication = self.replication.intersect(replication);
    }
}

/// Hashing function for group by operations
pub fn group_by_hash<T: Hash>(item: &T) -> u64 {
    let mut hasher = wyhash::WyHash::with_seed(0x0123456789abcdef);
    item.hash(&mut hasher);
    hasher.finish()
}

/// Hasher used for internal hashmaps that have coordinates as keys
/// (optimized for small keys)
pub type CoordHasherBuilder = fxhash::FxBuildHasher;

/// Hasher used for StreamElement keys
/// (for all around good performance)
pub type GroupHasherBuilder = core::hash::BuildHasherDefault<wyhash::WyHash>;
