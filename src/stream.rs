use parking_lot::Mutex;

use std::marker::PhantomData;
use std::sync::Arc;

use crate::block::{BatchMode, Block, NextStrategy, Scheduling};
use crate::environment::StreamContextInner;
use crate::operator::end::End;
use crate::operator::iteration::IterationStateLock;
use crate::operator::source::Source;
use crate::operator::window::WindowDescription;
use crate::operator::DataKey;
use crate::operator::Start;
use crate::operator::{Data, ExchangeData, KeyerFn, Operator};
use crate::scheduler::BlockId;

/// A Stream represents a chain of operators that work on a flow of data. The type of the elements
/// that is leaving the stream is `Out`.
///
/// Internally a stream is composed by a chain of blocks, each of which can be seen as a simpler
/// stream with input and output types.
///
/// A block is internally composed of a chain of operators, nested like the `Iterator` from `std`.
/// The type of the chain inside the block is `OperatorChain` and it's required as type argument of
/// the stream. This type only represents the chain inside the last block of the stream, not all the
/// blocks inside of it.
pub struct Stream<Op>
where
    Op: Operator,
{
    /// The last block inside the stream.
    pub(crate) block: Block<Op>,
    /// A reference to the environment this stream lives in.
    pub(crate) ctx: Arc<Mutex<StreamContextInner>>,
}

pub trait KeyedItem {
    type Key: DataKey;
    type Value;
    fn key(&self) -> &Self::Key;
    fn value(&self) -> &Self::Value;
    fn into_kv(self) -> (Self::Key, Self::Value);
}

impl<K: DataKey, V> KeyedItem for (K, V) {
    type Key = K;
    type Value = V;
    fn key(&self) -> &Self::Key {
        &self.0
    }
    fn value(&self) -> &Self::Value {
        &self.1
    }
    fn into_kv(self) -> (Self::Key, Self::Value) {
        self
    }
}

/// A [`KeyedStream`] is like a set of [`Stream`]s, each of which partitioned by some `Key`. Internally
/// it's just a stream whose elements are `(K, V)` pairs and the operators behave following the
/// [`KeyedStream`] semantics.
///
/// The type of the `Key` must be a valid key inside an hashmap.
pub struct KeyedStream<OperatorChain>(pub Stream<OperatorChain>)
where
    OperatorChain: Operator,
    OperatorChain::Out: KeyedItem;

/// A [`WindowedStream`] is a data stream partitioned by `Key`, where elements of each partition
/// are divided in groups called windows.
/// Each element can be assigned to one or multiple windows.
///
/// Windows are handled independently for each partition of the stream.
/// Each partition may be processed in parallel.
///
/// The windowing logic is implemented through 3 traits:
/// - A [`WindowDescription`] contains the parameters and logic that characterize the windowing strategy,
///   when given a `WindowAccumulator` it instantiates a `WindowManager`.
/// - A [`WindowManger`](crate::operator::window::WindowManager) is the struct responsible for creating
///   the windows and forwarding the input elements to the correct window which will should pass it to
///   its `WindowAccumulator`.
/// - A [`WindowAccumulator`](crate::operator::window::WindowAccumulator) contains the logic that should
///   be applied to the elements of each window.
///
/// There are a set of provided window descriptions with their respective managers:
///  - [`EventTimeWindow`][crate::operator::window::EventTimeWindow]
///  - [`ProcessingTimeWindow`][crate::operator::window::ProcessingTimeWindow]
///  - [`CountWindow`][crate::operator::window::CountWindow]
///  - [`SessionWindow`][crate::operator::window::SessionWindow]
///  - [`TransactionWindow`][crate::operator::window::TransactionWindow]
///
pub struct WindowedStream<Op, O: Data, WinDescr>
where
    Op: Operator,
    Op::Out: KeyedItem,
    WinDescr: WindowDescription<<Op::Out as KeyedItem>::Value>,
{
    pub(crate) inner: KeyedStream<Op>,
    pub(crate) descr: WinDescr,
    pub(crate) _win_out: PhantomData<O>,
}

impl<Op> Stream<Op>
where
    Op: Operator,
{
    pub(crate) fn new(ctx: Arc<Mutex<StreamContextInner>>, block: Block<Op>) -> Self {
        Self { block, ctx }
    }

    /// Add a new operator to the current chain inside the stream. This consumes the stream and
    /// returns a new one with the operator added.
    ///
    /// `get_operator` is a function that is given the previous chain of operators and should return
    /// the new chain of operators. The new chain cannot be simply passed as argument since it is
    /// required to do a partial move of the `InnerBlock` structure.
    ///
    /// **Note**: this is an advanced function that manipulates the block structure. Probably it is
    /// not what you are looking for.
    pub fn add_operator<Op2, GetOp>(self, get_operator: GetOp) -> Stream<Op2>
    where
        Op2: Operator,
        GetOp: FnOnce(Op) -> Op2,
    {
        Stream::new(self.ctx, self.block.add_operator(get_operator))
    }

    /// Add a new block to the stream, closing and registering the previous one. The new block is
    /// connected to the previous one.
    ///
    /// `get_end_operator` is used to extend the operator chain of the old block with the last
    /// operator (e.g. `operator::End`, `operator::GroupByEndOperator`). The end operator must
    /// be an `Operator<()>`.
    ///
    /// The new block is initialized with a `Start`.
    pub(crate) fn split_block<GetEndOp, OpEnd, IndexFn>(
        self,
        get_end_operator: GetEndOp,
        next_strategy: NextStrategy<Op::Out, IndexFn>,
    ) -> Stream<impl Operator<Out = Op::Out>>
    where
        IndexFn: KeyerFn<u64, Op::Out>,
        Op::Out: ExchangeData,
        OpEnd: Operator<Out = ()> + 'static,
        GetEndOp: FnOnce(Op, NextStrategy<Op::Out, IndexFn>, BatchMode) -> OpEnd,
    {
        let Stream { block, ctx } = self;
        // Clone parameters for new block
        let batch_mode = block.batch_mode;
        let iteration_ctx = block.iteration_ctx.clone();
        // Add end operator
        let mut block =
            block.add_operator(|prev| get_end_operator(prev, next_strategy.clone(), batch_mode));
        block.is_only_one_strategy = matches!(next_strategy, NextStrategy::OnlyOne);

        // Close old block
        let mut env_lock = ctx.lock();
        let prev_id = env_lock.close_block(block);
        // Create new block
        let source = Start::single(prev_id, iteration_ctx.last().cloned());
        let new_block = env_lock.new_block(source, batch_mode, iteration_ctx);
        // Connect blocks
        env_lock.connect_blocks::<Op::Out>(prev_id, new_block.id);

        drop(env_lock);
        Stream::new(ctx, new_block)
    }

    /// Similar to `.add_block`, but with 2 incoming blocks.
    ///
    /// This will add a new Y connection between two blocks. The two incoming blocks will be closed
    /// and a new one will be created with the 2 previous ones coming into it.
    ///
    /// This won't add any network shuffle, hence the next strategy will be `OnlyOne`. For this
    /// reason the 2 input streams must have the same parallelism, otherwise this function panics.
    ///
    /// The start operator of the new block must support multiple inputs: the provided function
    /// will be called with the ids of the 2 input blocks and should return the new start operator
    /// of the new block.
    pub(crate) fn binary_connection<Op2, S, Fs, F1, F2>(
        self,
        oth: Stream<Op2>,
        get_start_operator: Fs,
        next_strategy1: NextStrategy<Op::Out, F1>,
        next_strategy2: NextStrategy<Op2::Out, F2>,
    ) -> Stream<S>
    where
        Op: 'static,
        Op2: Operator + 'static,
        Op::Out: ExchangeData,
        Op2::Out: ExchangeData,
        F1: KeyerFn<u64, Op::Out>,
        F2: KeyerFn<u64, Op2::Out>,
        S: Operator + Source,
        Fs: FnOnce(BlockId, BlockId, bool, bool, Option<Arc<IterationStateLock>>) -> S,
    {
        let Stream { block: b1, ctx } = self;
        let Stream { block: b2, .. } = oth;

        let batch_mode = b1.batch_mode;
        let is_one_1 = matches!(next_strategy1, NextStrategy::OnlyOne);
        let is_one_2 = matches!(next_strategy2, NextStrategy::OnlyOne);
        let sched_1 = b1.scheduling.clone();
        let sched_2 = b2.scheduling.clone();
        if is_one_1 && is_one_2 && sched_1.replication != sched_2.replication {
            panic!(
                "The parallelism of the 2 blocks coming inside a Y connection must be equal. \
                On the left ({}) is {:?}, on the right ({}) is {:?}",
                b1, sched_1.replication, b2, sched_2.replication
            );
        }

        let iter_ctx_1 = b1.iteration_ctx();
        let iter_ctx_2 = b2.iteration_ctx();
        let (iteration_ctx, left_cache, right_cache) = if iter_ctx_1 == iter_ctx_2 {
            (b1.iteration_ctx.clone(), false, false)
        } else {
            if !iter_ctx_1.is_empty() && !iter_ctx_2.is_empty() {
                panic!("Side inputs are supported only if one of the streams is coming from outside any iteration");
            }
            if iter_ctx_1.is_empty() {
                // self is the side input, cache it
                (b2.iteration_ctx.clone(), true, false)
            } else {
                // oth is the side input, cache it
                (b1.iteration_ctx.clone(), false, true)
            }
        };

        // close previous blocks

        let mut b1 = b1.add_operator(|prev| End::new(prev, next_strategy1, batch_mode));
        let mut b2 = b2.add_operator(|prev| End::new(prev, next_strategy2, batch_mode));
        b1.is_only_one_strategy = is_one_1;
        b2.is_only_one_strategy = is_one_2;

        let mut env_lock = ctx.lock();
        let id_1 = b1.id;
        let id_2 = b2.id;

        env_lock.close_block(b1);
        env_lock.close_block(b2);

        let source = get_start_operator(
            id_1,
            id_2,
            left_cache,
            right_cache,
            iteration_ctx.last().cloned(),
        );

        let mut new_block = env_lock.new_block(source, batch_mode, iteration_ctx);
        let id_new = new_block.id;

        env_lock.connect_blocks::<Op::Out>(id_1, id_new);
        env_lock.connect_blocks::<Op2::Out>(id_2, id_new);

        drop(env_lock);

        // make sure the new block has the same parallelism of the previous one with OnlyOne
        // strategy
        new_block.scheduling = match (is_one_1, is_one_2) {
            (true, _) => sched_1,
            (_, true) => sched_2,
            _ => Scheduling::default(),
        };

        Stream::new(ctx, new_block)
    }

    /// Clone the given block, taking care of connecting the new block to the same previous blocks
    /// of the original one.
    pub(crate) fn clone(&mut self) -> Self {
        let new_block = self.ctx.lock().clone_block(&self.block);
        Stream::new(self.ctx.clone(), new_block)
    }

    /// Like `add_block` but without creating a new block. Therefore this closes the current stream
    /// and just add the last block to the scheduler.
    pub(crate) fn finalize_block(self)
    where
        Op: 'static,
        Op::Out: Send,
    {
        let mut env = self.ctx.lock();
        env.scheduler_mut().schedule_block(self.block);
    }
}

impl<OperatorChain> KeyedStream<OperatorChain>
where
    OperatorChain: Operator + 'static,
    OperatorChain::Out: KeyedItem,
{
    pub(crate) fn add_operator<Op2, GetOp>(self, get_operator: GetOp) -> KeyedStream<Op2>
    where
        Op2: Operator,
        GetOp: FnOnce(OperatorChain) -> Op2,
        Op2::Out: KeyedItem<Key = <OperatorChain::Out as KeyedItem>::Key>,
    {
        KeyedStream(self.0.add_operator(get_operator))
    }
}

impl<OperatorChain> Stream<OperatorChain>
where
    OperatorChain: Operator,
    OperatorChain::Out: KeyedItem,
{
    /// TODO DOCS
    pub fn to_keyed(self) -> KeyedStream<OperatorChain> {
        KeyedStream(self)
    }
}
