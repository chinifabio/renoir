//! Operators that can be applied to a stream.
//!
//! The actual operator list can be found from the implemented methods of [`Stream`](crate::Stream),
//! [`KeyedStream`](crate::KeyedStream), [`WindowedStream`](crate::WindowedStream)

use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::{AddAssign, Div};

#[cfg(feature = "crossbeam")]
use crossbeam_channel::{unbounded, Receiver, Sender};
#[cfg(not(feature = "crossbeam"))]
use flume::{unbounded, Receiver};
#[cfg(feature = "async-tokio")]
use futures::Future;
use serde::{Deserialize, Serialize};

pub(crate) use start::*;

pub use rich_map_custom::ElementGenerator;

use crate::block::{group_by_hash, BlockStructure, NextStrategy, Replication};
use crate::scheduler::ExecutionMetadata;

use crate::stream::KeyedItem;
use crate::{BatchMode, KeyedStream, Stream};

use self::cache::{CacheSink, StreamCache, StreamCacheRef};
use self::fold_batch::FoldBatch;
#[cfg(feature = "async-tokio")]
use self::map_async::MapAsync;
use self::map_memo::MapMemo;
use self::sink::collect::Collect;
use self::sink::collect_channel::CollectChannelSink;
use self::sink::collect_count::CollectCountSink;
use self::sink::collect_vec::CollectVecSink;
use self::sink::for_each::ForEach;
use self::sink::{StreamOutput, StreamOutputRef};
#[cfg(feature = "timestamp")]
use self::{
    add_timestamps::{AddTimestamp, DropTimestamp},
    interval_join::IntervalJoin,
};
use self::{
    end::End,
    filter::Filter,
    filter_map::FilterMap,
    flat_map::{FlatMap, KeyedFlatMap},
    flatten::{Flatten, KeyedFlatten},
    fold::Fold,
    inspect::Inspect,
    key_by::KeyBy,
    keyed_fold::KeyedFold,
    map::Map,
    merge::MergeElement,
    reorder::Reorder,
    rich_map::RichMap,
    rich_map_custom::RichMapCustom,
    route::RouterBuilder,
    zip::Zip,
};

#[cfg(feature = "timestamp")]
mod add_timestamps;
mod batch_mode;
pub mod cache;
mod covariance;
pub(crate) mod end;
mod entropy;
mod filter;
pub mod filter_expr;
mod filter_map;
mod flat_map;
mod flatten;
mod fold;
mod fold_batch;
mod inspect;
#[cfg(feature = "timestamp")]
mod interval_join;
pub mod iteration;
pub mod join;
pub(crate) mod key_by;
mod keyed_fold;
mod map;
#[cfg(feature = "async-tokio")]
mod map_async;
mod map_memo;
mod max;
mod mean;
mod median_exact;
mod merge;
mod min;
mod missing_data;
mod mode;
mod pearson;
mod quantile_approx;
mod reorder;
mod replication;
mod rich_map;
mod rich_map_custom;
mod route;
pub mod sink;
mod skewness_kurtosis;
pub mod source;
mod start;
mod variance;
pub mod window;
mod zip;

/// Marker trait that all the types inside a stream should implement.
pub trait Data: Clone + Send + 'static {}
impl<T: Clone + Send + 'static> Data for T {}

/// Marker trait for data types that are used to communicate between different blocks.
pub trait ExchangeData: Serialize + for<'a> Deserialize<'a> + Clone + Send + 'static {}
impl<T: Serialize + for<'a> Deserialize<'a> + Clone + Send + 'static> ExchangeData for T {}

/// Marker trait that all the keys should implement.
pub trait DataKey: Clone + Send + Hash + Eq + 'static {}
impl<T: Clone + Send + Hash + Eq + 'static> DataKey for T {}

/// Marker trait for key types that are used when communicating between different blocks.
pub trait ExchangeDataKey: DataKey + ExchangeData {}
impl<T: DataKey + ExchangeData> ExchangeDataKey for T {}

/// Marker trait for the function that extracts the key out of a type.
pub trait KeyerFn<Key, Out>: Fn(&Out) -> Key + Clone + Send + 'static {}
impl<Key, Out, T: Fn(&Out) -> Key + Clone + Send + 'static> KeyerFn<Key, Out> for T {}

/// When using timestamps and watermarks, this type expresses the timestamp of a message or of a
/// watermark.
#[cfg(feature = "timestamp")]
pub type Timestamp = i64;

#[cfg(not(feature = "timestamp"))]
pub type Timestamp = ();

/// An element of the stream. This is what enters and exits from the operators.
///
/// An operator may need to change the content of a `StreamElement` (e.g. a `Map` may change the
/// value of the `Item`). Usually `Watermark` and `FlushAndRestart` are simply forwarded to the next
/// operator in the chain.
///
/// In general a stream may be composed of a sequence of this kind:
///
/// `((Item | Timestamped | Watermark | FlushBatch)* FlushAndRestart)+ Terminate`
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub enum StreamElement<Out> {
    /// A normal element containing just the value of the message.
    Item(Out),
    /// Like `Item`, but it's attached with a timestamp, it's used to ensure the ordering of the
    /// messages.
    Timestamped(Out, Timestamp),
    /// When an operator receives a `Watermark` with timestamp `t`, the operator will never see any
    /// message with timestamp less or equal to `t`.
    Watermark(Timestamp),
    /// Flush the internal batch since there will be too much delay till the next message to come.
    FlushBatch,
    /// The stream has ended, and the operators should exit as soon as possible.
    ///
    /// No messages should be generated by the operator between a `FlushAndRestart` and a
    /// `Terminate`.
    Terminate,
    /// Mark the end of a stream of data.
    ///
    /// Note that this does not mean that the entire stream has ended, for example this is used to
    /// mark the end of an iteration. Therefore an operator may be prepared to received new data
    /// after this message, but should not retain the internal state.
    FlushAndRestart,
}

/// An operator represents a unit of computation. It's always included inside a chain of operators,
/// inside a block.
///
/// Each operator implements the `Operator<Out>` trait, it produced a stream of `Out` elements.
///
/// An `Operator` must be Clone since it is part of a single chain when it's built, but it has to
/// be cloned to spawn the replicas of the block.
pub trait Operator: Clone + Send + Display {
    type Out: Send;
    /// Setup the operator chain. This is called before any call to `next` and it's used to
    /// initialize the operator. When it's called the operator has already been cloned and it will
    /// never be cloned again. Therefore it's safe to store replica-specific metadata inside of it.
    ///
    /// It's important that each operator (except the start of a chain) calls `.setup()` recursively
    /// on the previous operators.
    fn setup(&mut self, metadata: &mut ExecutionMetadata);

    /// Take a value from the previous operator, process it and return it.
    fn next(&mut self) -> StreamElement<Self::Out>;

    /// A more refined representation of the operator and its predecessors.
    fn structure(&self) -> BlockStructure;
}

impl<Out> StreamElement<Out> {
    /// Create a new `StreamElement` with an `Item(())` if `self` contains an item, otherwise it
    /// returns the same variant of `self`.
    pub fn take(&self) -> StreamElement<()> {
        match self {
            StreamElement::Item(_) => StreamElement::Item(()),
            StreamElement::Timestamped(_, _) => StreamElement::Item(()),
            StreamElement::Watermark(w) => StreamElement::Watermark(*w),
            StreamElement::Terminate => StreamElement::Terminate,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
        }
    }

    /// Change the type of the element inside the `StreamElement`.
    pub fn map<NewOut>(self, f: impl FnOnce(Out) -> NewOut) -> StreamElement<NewOut> {
        match self {
            StreamElement::Item(item) => StreamElement::Item(f(item)),
            StreamElement::Timestamped(item, ts) => StreamElement::Timestamped(f(item), ts),
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => StreamElement::Terminate,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
        }
    }

    /// Change the type of the element inside the `StreamElement`.
    #[cfg(feature = "async-tokio")]
    pub async fn map_async<NewOut, F, Fut>(self, f: F) -> StreamElement<NewOut>
    where
        F: FnOnce(Out) -> Fut,
        Fut: Future<Output = NewOut>,
    {
        match self {
            StreamElement::Item(item) => StreamElement::Item(f(item).await),
            StreamElement::Timestamped(item, ts) => StreamElement::Timestamped(f(item).await, ts),
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => StreamElement::Terminate,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
        }
    }

    /// A string representation of the variant of this `StreamElement`.
    pub fn variant(&self) -> &'static str {
        match self {
            StreamElement::Item(_) => "Item",
            StreamElement::Timestamped(_, _) => "Timestamped",
            StreamElement::Watermark(_) => "Watermark",
            StreamElement::FlushBatch => "FlushBatch",
            StreamElement::Terminate => "Terminate",
            StreamElement::FlushAndRestart => "FlushAndRestart",
        }
    }

    /// A string representation of the variant of this `StreamElement`.
    pub fn timestamp(&self) -> Option<&Timestamp> {
        match self {
            StreamElement::Timestamped(_, ts) | StreamElement::Watermark(ts) => Some(ts),
            _ => None,
        }
    }

    pub fn add_key<Key>(self, k: Key) -> StreamElement<(Key, Out)> {
        match self {
            StreamElement::Item(v) => StreamElement::Item((k, v)),
            StreamElement::Timestamped(v, ts) => StreamElement::Timestamped((k, v), ts),
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => StreamElement::Terminate,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
        }
    }

    pub fn value(&self) -> Option<&Out> {
        match self {
            StreamElement::Item(v) => Some(v),
            StreamElement::Timestamped(v, _) => Some(v),
            StreamElement::Watermark(_) => None,
            StreamElement::FlushBatch => None,
            StreamElement::Terminate => None,
            StreamElement::FlushAndRestart => None,
        }
    }
}

impl<Key, Out> StreamElement<(Key, Out)> {
    /// Map a `StreamElement<KeyValue(Key, Out)>` to a `StreamElement<Out>`,
    /// returning the key if possible
    pub fn take_key(self) -> (Option<Key>, StreamElement<Out>) {
        match self {
            StreamElement::Item((k, v)) => (Some(k), StreamElement::Item(v)),
            StreamElement::Timestamped((k, v), ts) => (Some(k), StreamElement::Timestamped(v, ts)),
            StreamElement::Watermark(w) => (None, StreamElement::Watermark(w)),
            StreamElement::Terminate => (None, StreamElement::Terminate),
            StreamElement::FlushAndRestart => (None, StreamElement::FlushAndRestart),
            StreamElement::FlushBatch => (None, StreamElement::FlushBatch),
        }
    }

    pub fn key(self) -> Option<Key> {
        match self {
            StreamElement::Item((k, _)) => Some(k),
            StreamElement::Timestamped((k, _), _) => Some(k),
            StreamElement::Watermark(_) => None,
            StreamElement::Terminate => None,
            StreamElement::FlushAndRestart => None,
            StreamElement::FlushBatch => None,
        }
    }
}

impl<Op> Stream<Op>
where
    Op: Operator + 'static,
{
    /// Given a stream without timestamps nor watermarks, tag each item with a timestamp and insert
    /// watermarks.
    ///
    /// The two functions given to this operator are the following:
    /// - `timestamp_gen` returns the timestamp assigned to the provided element of the stream
    /// - `watermark_gen` returns an optional watermark to add after the provided element
    ///
    /// Note that the two functions **must** follow the watermark semantics.
    /// TODO: link to watermark semantics
    ///
    /// ## Example
    ///
    /// In this example the stream contains the integers from 0 to 9, each will be tagged with a
    /// timestamp with the value of the item as milliseconds, and after each even number a watermark
    /// will be inserted.
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// use noir_compute::operator::Timestamp;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    ///
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// s.add_timestamps(
    ///     |&n| n,
    ///     |&n, &ts| if n % 2 == 0 { Some(ts) } else { None }
    /// );
    /// ```
    #[cfg(feature = "timestamp")]
    pub fn add_timestamps<F, G>(
        self,
        timestamp_gen: F,
        watermark_gen: G,
    ) -> Stream<AddTimestamp<F, G, Op>>
    where
        F: FnMut(&Op::Out) -> Timestamp + Clone + Send + 'static,
        G: FnMut(&Op::Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
    {
        self.add_operator(|prev| AddTimestamp::new(prev, timestamp_gen, watermark_gen))
    }

    #[cfg(feature = "timestamp")]
    pub fn drop_timestamps(self) -> Stream<DropTimestamp<Op>> {
        self.add_operator(|prev| DropTimestamp::new(prev))
    }
    /// Change the batch mode for this stream.
    ///
    /// This change will be propagated to all the operators following, even of the next blocks,
    /// until it's changed again.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// use noir_compute::BatchMode;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    ///
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// s.batch_mode(BatchMode::fixed(1024));
    /// ```
    pub fn batch_mode(mut self, batch_mode: BatchMode) -> Self {
        self.block.batch_mode = batch_mode;
        self
    }

    /// Remove from the stream all the elements for which the provided function returns `None` and
    /// keep the elements that returned `Some(_)`.
    ///
    /// **Note**: this is very similar to [`Iteartor::filter_map`](std::iter::Iterator::filter_map)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// let res = s.filter_map(|n| if n % 2 == 0 { Some(n * 3) } else { None }).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 6, 12, 18, 24])
    /// ```
    pub fn filter_map<O, F>(self, f: F) -> Stream<impl Operator<Out = O>>
    where
        F: Fn(Op::Out) -> Option<O> + Send + Clone + 'static,
        O: Data,
    {
        self.add_operator(|prev| FilterMap::new(prev, f))
    }

    /// Remove from the stream all the elements for which the provided predicate returns `false`.
    ///
    /// **Note**: this is very similar to [`Iteartor::filter`](std::iter::Iterator::filter)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// let res = s.filter(|&n| n % 2 == 0).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 2, 4, 6, 8])
    /// ```
    pub fn filter<F>(self, predicate: F) -> Stream<impl Operator<Out = Op::Out>>
    where
        F: Fn(&Op::Out) -> bool + Clone + Send + 'static,
    {
        self.add_operator(|prev| Filter::new(prev, predicate))
    }

    /// Reorder timestamped items
    ///
    /// # Example
    /// ### TODO
    pub fn reorder(self) -> Stream<impl Operator<Out = Op::Out>> {
        self.add_operator(|prev| Reorder::new(prev))
    }

    /// Remove from the stream all the elements for which the provided function returns `None` and
    /// keep the elements that returned `Some(_)`. The mapping function can be stateful.
    ///
    /// This is equivalent to [`Stream::filter_map`] but with a stateful function.
    ///
    /// Since the mapping function can be stateful, it is a `FnMut`. This allows expressing simple
    /// algorithms with very few lines of code (see examples).
    ///
    /// The mapping function is _cloned_ inside each replica, and they will not share state between
    /// each other. If you want that only a single replica handles all the items you may want to
    /// change the parallelism of this operator with [`Stream::replication`].
    ///
    /// ## Examples
    ///
    /// This will emit only the _positive prefix-sums_.
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(std::array::IntoIter::new([1, 2, -5, 3, 1])));
    /// let res = s.rich_filter_map({
    ///     let mut sum = 0;
    ///     move |x| {
    ///         sum += x;
    ///         if sum >= 0 {
    ///             Some(sum)
    ///         } else {
    ///             None
    ///         }
    ///     }
    /// }).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![1, 1 + 2, /* 1 + 2 - 5, */ 1 + 2 - 5 + 3, 1 + 2 - 5 + 3 + 1]);
    /// ```
    pub fn rich_filter_map<O, F>(self, f: F) -> Stream<impl Operator<Out = O>>
    where
        F: FnMut(Op::Out) -> Option<O> + Send + Clone + 'static,
        O: Data,
    {
        self.rich_map(f).filter(|x| x.is_some()).map(|x| x.unwrap())
    }

    /// Map the elements of the stream into new elements. The mapping function can be stateful.
    ///
    /// This is equivalent to [`Stream::map`] but with a stateful function.
    ///
    /// Since the mapping function can be stateful, it is a `FnMut`. This allows expressing simple
    /// algorithms with very few lines of code (see examples).
    ///
    /// The mapping function is _cloned_ inside each replica, and they will not share state between
    /// each other. If you want that only a single replica handles all the items you may want to
    /// change the parallelism of this operator with [`Stream::replication`].
    ///
    /// ## Examples
    ///
    /// This is a simple implementation of the prefix-sum using a single replica (i.e. each element
    /// is mapped to the sum of all the elements up to that point). Note that this won't work if
    /// there are more replicas.
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((1..=5)));
    /// let res = s.rich_map({
    ///     let mut sum = 0;
    ///     move |x| {
    ///         sum += x;
    ///         sum
    ///     }
    /// }).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![1, 1 + 2, 1 + 2 + 3, 1 + 2 + 3 + 4, 1 + 2 + 3 + 4 + 5]);
    /// ```    
    ///
    /// This will enumerate all the elements that reach a replica. This is basically equivalent to
    /// the `enumerate` function in Python.
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((1..=5)));
    /// let res = s.rich_map({
    ///     let mut id = 0;
    ///     move |x| {
    ///         id += 1;
    ///         (id - 1, x)
    ///     }
    /// }).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]);
    /// ```
    pub fn rich_map<O, F>(self, mut f: F) -> Stream<impl Operator<Out = O>>
    where
        F: FnMut(Op::Out) -> O + Send + Clone + 'static,
        O: Send + 'static,
    {
        self.key_by(|_| ())
            .add_operator(|prev| RichMap::new(prev, move |(_, value)| f(value)))
            .drop_key()
    }

    /// Map the elements of the stream into new elements.
    ///
    /// **Note**: this is very similar to [`Iteartor::map`](std::iter::Iterator::map).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.map(|n| n * 10).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 10, 20, 30, 40]);
    /// ```
    pub fn map<O: Send, F>(self, f: F) -> Stream<impl Operator<Out = O>>
    where
        F: Fn(Op::Out) -> O + Send + Clone + 'static,
    {
        self.add_operator(|prev| Map::new(prev, f))
    }

    /// Map the elements of the stream into new elements by evaluating a future for each one.
    /// Use memoization to cache outputs for previously seen inputs.
    ///
    /// The cache is implemented through a *per-process* [`quick_cache::sync::Cache`].
    /// The maximum number of elements to be cached is passed as the `capacity` parameter.
    ///
    /// The outputs are cached according to the key produced by the `fk` function.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # tokio::runtime::Runtime::new()
    /// #    .unwrap()
    /// #    .block_on(base());
    /// # async fn base() {
    /// #    let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream_iter(5..15);
    /// let res = s.map_async_memo_by(
    ///     |n| async move {(n * n) % 7}, |n| n % 7, 5
    /// ).collect_vec();
    /// env.execute().await;
    /// assert_eq!(res.get().unwrap(), vec![4, 1, 0, 1, 4, 2, 2, 4, 1, 0]);
    /// # }
    /// ```
    #[cfg(feature = "async-tokio")]
    pub fn map_async_memo_by<O, K, F, Fk, Fut>(
        self,
        f: F,
        fk: Fk,
        capacity: usize,
    ) -> Stream<impl Operator<Out = O>>
    where
        F: Fn(Op::Out) -> Fut + Send + Sync + 'static + Clone,
        Fk: Fn(&Op::Out) -> K + Send + Sync + Clone + 'static,
        Fut: futures::Future<Output = O> + Send,
        O: Clone + Send + Sync + 'static,
        K: DataKey + Sync,
    {
        use crate::block::GroupHasherBuilder;
        use futures::FutureExt;
        use quick_cache::{sync::Cache, UnitWeighter};
        use std::{convert::Infallible, sync::Arc};

        let cache: Arc<Cache<K, O, _, GroupHasherBuilder>> = Arc::new(Cache::with(
            capacity,
            capacity as u64,
            UnitWeighter,
            Default::default(),
            Default::default(),
        ));
        self.add_operator(|prev| {
            MapAsync::new(
                prev,
                move |el| {
                    let fk = fk.clone();
                    let f = f.clone();
                    let cache = cache.clone();
                    let k = fk(&el);
                    async move {
                        cache
                            .get_or_insert_async(&k, (f)(el).map(Result::Ok::<_, Infallible>))
                            .await
                            .unwrap()
                    }
                },
                4,
            )
        })
    }

    /// Map the elements of the stream into new elements by evaluating a future for each one.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # tokio::runtime::Runtime::new()
    /// #    .unwrap()
    /// #    .block_on(base());
    /// # async fn base() {
    /// #    let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream_iter(5..15);
    /// let res = s.map_async(|n| async move {(n * n) % 7}).collect_vec();
    /// env.execute().await;
    /// assert_eq!(res.get().unwrap(), vec![4, 1, 0, 1, 4, 2, 2, 4, 1, 0]);
    /// # }
    /// ```
    #[cfg(feature = "async-tokio")]
    pub fn map_async<O: Data, F, Fut>(self, f: F) -> Stream<impl Operator<Out = O>>
    where
        F: Fn(Op::Out) -> Fut + Send + Sync + 'static + Clone,
        Fut: futures::Future<Output = O> + Send + 'static,
    {
        self.add_operator(|prev| MapAsync::new(prev, f, 4))
    }

    /// Map the elements of the stream into new elements. Use memoization
    /// to cache outputs for previously seen inputs.
    ///
    /// The cache is implemented through a *per-process* [`quick_cache::sync::Cache`].
    /// The maximum number of elements to be cached is passed as the `capacity` parameter.
    ///
    /// The outputs are cached according to the key produced by the `fk` function.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((5..15)));
    /// let res = s.map_memo_by(|n| (n * n) % 7, |n| n % 7, 5).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![4, 1, 0, 1, 4, 2, 2, 4, 1, 0]);
    /// ```
    pub fn map_memo_by<K: DataKey + Sync, O: Clone + Send + Sync + 'static, F, Fk>(
        self,
        f: F,
        fk: Fk,
        capacity: usize,
    ) -> Stream<impl Operator<Out = O>>
    where
        F: Fn(Op::Out) -> O + Send + Clone + 'static,
        Fk: Fn(&Op::Out) -> K + Send + Clone + 'static,
    {
        self.add_operator(|prev| MapMemo::new(prev, f, fk, capacity))
    }

    /// Fold the stream into a stream that emits a single value.
    ///
    /// The folding operator consists in adding to the current accumulation value (initially the
    /// value provided as `init`) the value of the current item in the stream.
    ///
    /// The folding function is provided with a mutable reference to the current accumulator and the
    /// owned item of the stream. The function should modify the accumulator without returning
    /// anything.
    ///
    /// Note that the output type may be different from the input type. Consider using
    /// [`Stream::reduce`] if the output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator is not parallelized, it creates a bottleneck where all the stream
    /// elements are sent to and the folding is done using a single thread.
    ///
    /// **Note**: this is very similar to [`Iteartor::fold`](std::iter::Iterator::fold).
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.fold(0, |acc, value| *acc += value).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn fold<O, F>(self, init: O, f: F) -> Stream<impl Operator<Out = O>>
    where
        F: Fn(&mut O, Op::Out) + Send + Clone + 'static,
        Op::Out: ExchangeData,
        O: Send + Clone,
    {
        self.replication(Replication::One)
            .add_operator(|prev| Fold::new(prev, init, f))
    }

    /// Fold the stream into a stream that emits a single value.
    ///
    /// The folding operator consists in adding to the current accumulation value (initially the
    /// value provided as `init`) the value of the current item in the stream.
    ///
    /// This method is very similary to [`Stream::fold`], but performs the folding distributely. To
    /// do so the folding function must be _associative_, in particular the folding process is
    /// performed in 2 steps:
    ///
    /// - `local`: the local function is used to fold the elements present in each replica of the
    ///   stream independently. All those replicas will start with the same `init` value.
    /// - `global`: all the partial results (the elements produced by the `local` step) have to be
    ///   aggregated into a single result. This is done using the `global` folding function.
    ///
    /// Note that the output type may be different from the input type, therefore requireing
    /// different function for the aggregation. Consider using [`Stream::reduce_assoc`] if the
    /// output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.fold_assoc(0, |acc, value| *acc += value, |acc, value| *acc += value).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn fold_assoc<O, F, G>(self, init: O, local: F, global: G) -> Stream<impl Operator<Out = O>>
    where
        F: Fn(&mut O, Op::Out) + Send + Clone + 'static,
        G: Fn(&mut O, O) + Send + Clone + 'static,
        O: ExchangeData,
    {
        self.add_operator(|prev| Fold::new(prev, init.clone(), local))
            .replication(Replication::One)
            .add_operator(|prev| Fold::new(prev, init, global))
    }

    pub fn fold_batch<O, F>(
        self,
        init: O,
        f: F,
        batch_size: usize,
    ) -> Stream<impl Operator<Out = O>>
    where
        Op::Out: ExchangeData,
        F: Fn(&mut O, Vec<Op::Out>) + Send + Clone + 'static,
        O: Data,
    {
        self.replication(Replication::One)
            .add_operator(|prev| FoldBatch::new(prev, init, f, batch_size))
    }

    pub fn fold_batch_assoc<O, F, G>(
        self,
        init: O,
        local: F,
        global: G,
        local_batch_size: usize,
        global_batch_size: usize,
    ) -> Stream<impl Operator<Out = O>>
    where
        Op::Out: ExchangeData,
        F: Fn(&mut O, Vec<Op::Out>) + Send + Clone + 'static,
        G: Fn(&mut O, Vec<O>) + Send + Clone + 'static,
        O: ExchangeData,
    {
        self.add_operator(|prev| FoldBatch::new(prev, init.clone(), local, local_batch_size))
            .replication(Replication::One)
            .add_operator(|prev| FoldBatch::new(prev, init, global, global_batch_size))
    }

    /// Perform the folding operation separately for each key.
    ///
    /// This is equivalent of partitioning the stream using the `keyer` function, and then applying
    /// [`Stream::fold_assoc`] to each partition separately.
    ///
    /// Note however that there is a difference between `stream.group_by(keyer).fold(...)` and
    /// `stream.group_by_fold(keyer, ...)`. The first performs the network shuffle of every item in
    /// the stream, and **later** performs the folding (i.e. nearly all the elements will be sent to
    /// the network). The latter avoids sending the items by performing first a local reduction on
    /// each host, and then send only the locally folded results (i.e. one message per replica, per
    /// key); then the global step is performed aggregating the results.
    ///
    /// The resulting stream will still be keyed and will contain only a single message per key (the
    /// final result).
    ///
    /// Note that the output type may be different from the input type, therefore requireing
    /// different function for the aggregation. Consider using [`Stream::group_by_reduce`] if the
    /// output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_fold(|&n| n % 2, 0, |acc, value| *acc += value, |acc, value| *acc += value)
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
    /// ```
    pub fn group_by_fold<K, O, Fk, F, G>(
        self,
        keyer: Fk,
        init: O,
        local: F,
        global: G,
    ) -> KeyedStream<impl Operator<Out = (K, O)>>
    where
        Fk: Fn(&Op::Out) -> K + Send + Clone + 'static,
        F: Fn(&mut O, Op::Out) + Send + Clone + 'static,
        G: Fn(&mut O, O) + Send + Clone + 'static,
        K: ExchangeDataKey,
        O: ExchangeData,
        Op::Out: Clone,
    {
        // GroupBy based on key
        let next_strategy = NextStrategy::GroupBy(
            move |(key, _): &(K, O)| group_by_hash(&key),
            Default::default(),
        );

        let new_stream = self
            // key_by with given keyer
            .add_operator(|prev| KeyBy::new(prev, keyer.clone()))
            // local fold
            .add_operator(|prev| KeyedFold::new(prev, init.clone(), local))
            // group by key
            .split_block(End::new, next_strategy)
            // global fold
            .add_operator(|prev| KeyedFold::new(prev, init.clone(), global));

        KeyedStream(new_stream)
    }

    /// Construct a [`KeyedStream`] from a [`Stream`] without shuffling the data.
    ///
    /// **Note**: this violates the semantics of [`KeyedStream`], without sending all the values
    /// with the same key to the same replica some of the following operators may misbehave. You
    /// probably need to use [`Stream::group_by`] instead.
    ///
    /// ## Example
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.key_by(|&n| n % 2).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 2), (0, 4), (1, 1), (1, 3)]);
    /// ```
    pub fn key_by<K, Fk>(self, keyer: Fk) -> KeyedStream<impl Operator<Out = (K, Op::Out)>>
    where
        Fk: Fn(&Op::Out) -> K + Send + Clone + 'static,
        K: DataKey,
    {
        KeyedStream(self.add_operator(|prev| KeyBy::new(prev, keyer)))
    }

    /// Apply the given function to all the elements of the stream, consuming the stream.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// s.inspect(|n| println!("Item: {}", n)).for_each(std::mem::drop);
    ///
    /// env.execute_blocking();
    /// ```
    pub fn inspect<F>(self, f: F) -> Stream<impl Operator<Out = Op::Out>>
    where
        F: FnMut(&Op::Out) + Send + Clone + 'static,
    {
        self.add_operator(|prev| Inspect::new(prev, f))
    }

    /// Apply a mapping operation to each element of the stream, the resulting stream will be the
    /// flattened values of the result of the mapping. The mapping function can be stateful.
    ///
    /// This is equivalent to [`Stream::flat_map`] but with a stateful function.
    ///
    /// Since the mapping function can be stateful, it is a `FnMut`. This allows expressing simple
    /// algorithms with very few lines of code (see examples).
    ///
    /// The mapping function is _cloned_ inside each replica, and they will not share state between
    /// each other. If you want that only a single replica handles all the items you may want to
    /// change the parallelism of this operator with [`Stream::replication`].
    ///
    /// ## Examples
    ///
    /// This will emit only the _positive prefix-sums_.
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..=3)));
    /// let res = s.rich_flat_map({
    ///     let mut elements = Vec::new();
    ///     move |y| {
    ///         let new_pairs = elements
    ///             .iter()
    ///             .map(|&x: &u32| (x, y))
    ///             .collect::<Vec<_>>();
    ///         elements.push(y);
    ///         new_pairs
    ///     }
    /// }).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![(0, 1), (0, 2), (1, 2), (0, 3), (1, 3), (2, 3)]);
    /// ```
    pub fn rich_flat_map<It, F>(self, f: F) -> Stream<impl Operator<Out = It::Item>>
    where
        It: IntoIterator + Send + 'static,
        <It as IntoIterator>::IntoIter: Send + 'static,
        <It as IntoIterator>::Item: Send,
        F: FnMut(Op::Out) -> It + Send + Clone + 'static,
    {
        self.rich_map(f).flatten()
    }

    /// Map the elements of the stream into new elements. The mapping function can be stateful.
    ///
    /// This version of `rich_flat_map` is a lower level primitive that gives full control over the
    /// inner types used in streams. It can be used to define custom unary operators.
    ///
    /// The closure must follow these rules to ensure the correct behaviour of noir:
    /// + `Watermark` messages must be sent when no more items with lower timestamp will ever be produced
    /// + `FlushBatch` messages must be forwarded if received
    /// + For each `FlushAndRestart` and `Terminate` message received, the operator must generate
    ///     one and only one message of the same kind. No other messages of this kind should be created
    ///
    /// The mapping function is _cloned_ inside each replica, and they will not share state between
    /// each other. If you want that only a single replica handles all the items you may want to
    /// change the parallelism of this operator with [`Stream::replication`].
    ///
    /// ## Examples
    ///
    /// TODO
    pub fn rich_map_custom<O, F>(self, f: F) -> Stream<impl Operator<Out = O>>
    where
        F: FnMut(ElementGenerator<Op>) -> StreamElement<O> + Clone + Send + 'static,
        O: Send,
    {
        self.add_operator(|prev| RichMapCustom::new(prev, f))
    }

    /// Apply a mapping operation to each element of the stream, the resulting stream will be the
    /// flatMaped values of the result of the mapping.
    ///
    /// **Note**: this is very similar to [`Iteartor::flat_map`](std::iter::Iterator::flat_map)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..3)));
    /// let res = s.flat_map(|n| vec![n, n]).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 0, 1, 1, 2, 2]);
    /// ```
    pub fn flat_map<It, F>(self, f: F) -> Stream<impl Operator<Out = It::Item>>
    where
        It: IntoIterator + 'static,
        It::IntoIter: Send + 'static,
        It::Item: Send,
        F: Fn(Op::Out) -> It + Send + Clone + 'static,
    {
        self.add_operator(|prev| FlatMap::new(prev, f))
    }

    /// Apply the given function to all the elements of the stream, consuming the stream.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// s.for_each(|n| println!("Item: {}", n));
    ///
    /// env.execute_blocking();
    /// ```
    pub fn for_each<F>(self, f: F)
    where
        F: FnMut(Op::Out) + Send + Clone + 'static,
    {
        self.add_operator(|prev| ForEach::new(prev, f))
            .finalize_block();
    }

    /// Transform this stream of containers into a stream of all the contained values.
    ///
    /// **Note**: this is very similar to [`Iteartor::flatten`](std::iter::Iterator::flatten)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(vec![
    ///     vec![1, 2, 3],
    ///     vec![],
    ///     vec![4, 5],
    /// ].into_iter()));
    /// let res = s.flatten().collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![1, 2, 3, 4, 5]);
    /// ```
    pub fn flatten(self) -> Stream<impl Operator<Out = <Op::Out as IntoIterator>::Item>>
    where
        Op::Out: IntoIterator,
        <Op::Out as IntoIterator>::IntoIter: Send,
        <Op::Out as IntoIterator>::Item: Send,
    {
        self.add_operator(|prev| Flatten::new(prev))
    }
}

impl<I, Op> Stream<Op>
where
    I: ExchangeData,
    Op: Operator<Out = I> + 'static,
{
    /// Duplicate each element of the stream and forward it to all the replicas of the next block.
    ///
    /// **Note**: this will duplicate the elements of the stream, this is potentially a very
    /// expensive operation.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// s.broadcast();
    /// ```
    pub fn broadcast(self) -> Stream<impl Operator<Out = Op::Out>> {
        self.split_block(End::new, NextStrategy::all())
    }

    /// Given a stream, make a [`KeyedStream`] partitioning the values according to a key generated
    /// by the `keyer` function provided.
    ///
    /// The returned [`KeyedStream`] is partitioned by key, and all the operators added to it will
    /// be evaluated _after_ the network shuffle. Therefore all the items are sent to the network
    /// (if their destination is not the local host). In many cases this behaviour can be avoided by
    /// using the associative variant of the operators (e.g. [`Stream::group_by_reduce`],
    /// [`Stream::group_by_sum`], ...).
    ///
    /// **Note**: the keys are not sent to the network, they are built on the sending side, and
    /// rebuilt on the receiving side.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let keyed = s.group_by(|&n| n % 2); // partition even and odd elements
    /// ```
    pub fn group_by<K, Fk>(self, keyer: Fk) -> KeyedStream<impl Operator<Out = (K, I)>>
    where
        Fk: Fn(&Op::Out) -> K + Send + Clone + 'static,
        K: DataKey,
    {
        let next_strategy = NextStrategy::group_by(keyer.clone());
        let new_stream = self
            .split_block(End::new, next_strategy)
            .add_operator(|prev| KeyBy::new(prev, keyer));
        KeyedStream(new_stream)
    }

    /// Find, for each partition of the stream, the item with the largest value.
    ///
    /// The stream is partitioned using the `keyer` function and the value to compare is obtained
    /// with `get_value`.
    ///
    /// This operation is associative, therefore the computation is done in parallel before sending
    /// all the elements to the network.
    ///
    /// **Note**: the comparison is done using the value returned by `get_value`, but the resulting
    /// items have the same type as the input.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_max_element(|&n| n % 2, |&n| n)
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 4), (1, 3)]);
    /// ```
    pub fn group_by_max_element<K, V, Fk, Fv>(
        self,
        keyer: Fk,
        get_value: Fv,
    ) -> KeyedStream<impl Operator<Out = (K, I)>>
    where
        Fk: KeyerFn<K, Op::Out> + Fn(&Op::Out) -> K,
        Fv: KeyerFn<V, Op::Out> + Fn(&Op::Out) -> V,
        K: ExchangeDataKey,
        V: Ord,
    {
        self.group_by_reduce(keyer, move |out, value| {
            if get_value(&value) > get_value(out) {
                *out = value;
            }
        })
    }

    /// Find, for each partition of the stream, the sum of the values of the items.
    ///
    /// The stream is partitioned using the `keyer` function and the value to sum is obtained
    /// with `get_value`.
    ///
    /// This operation is associative, therefore the computation is done in parallel before sending
    /// all the elements to the network.
    ///
    /// **Note**: this is similar to the SQL: `SELECT SUM(value) ... GROUP BY key`
    ///
    /// **Note**: the type of the result does not have to be a number, any type that implements
    /// `AddAssign` is accepted.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_sum(|&n| n % 2, |n| n)
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
    /// ```
    pub fn group_by_sum<K, V, Fk, Fv>(
        self,
        keyer: Fk,
        get_value: Fv,
    ) -> KeyedStream<impl Operator<Out = (K, V)>>
    where
        Fk: KeyerFn<K, Op::Out> + Fn(&Op::Out) -> K,
        Fv: Fn(Op::Out) -> V + Clone + Send + 'static,
        V: ExchangeData + AddAssign,
        K: ExchangeDataKey,
    {
        let s = self.group_by_fold(
            keyer,
            None,
            move |acc, value| {
                if let Some(acc) = acc {
                    *acc += get_value(value);
                } else {
                    *acc = Some(get_value(value));
                }
            },
            |acc, value| match acc {
                None => *acc = value,
                Some(acc) => {
                    if let Some(value) = value {
                        *acc += value
                    }
                }
            },
        );
        s.map(|(_, o)| o.unwrap())
    }

    /// Find, for each partition of the stream, the average of the values of the items.
    ///
    /// The stream is partitioned using the `keyer` function and the value to average is obtained
    /// with `get_value`.
    ///
    /// This operation is associative, therefore the computation is done in parallel before sending
    /// all the elements to the network.
    ///
    /// **Note**: this is similar to the SQL: `SELECT AVG(value) ... GROUP BY key`
    ///
    /// **Note**: the type of the result does not have to be a number, any type that implements
    /// `AddAssign` and can be divided by `f64` is accepted.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_avg(|&n| n % 2, |&n| n as f64)
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_by_key(|(k, _)| *k);
    /// assert_eq!(res, vec![(0, (0.0 + 2.0 + 4.0) / 3.0), (1, (1.0 + 3.0) / 2.0)]);
    /// ```
    pub fn group_by_avg<K, V, Fk, Fv>(
        self,
        keyer: Fk,
        get_value: Fv,
    ) -> KeyedStream<impl Operator<Out = (K, V)>>
    where
        Fk: KeyerFn<K, Op::Out> + Fn(&Op::Out) -> K,
        Fv: KeyerFn<V, Op::Out> + Fn(&Op::Out) -> V,
        V: ExchangeData + AddAssign + Div<f64, Output = V>,
        K: ExchangeDataKey,
    {
        self.group_by_fold(
            keyer,
            (None, 0usize),
            move |(sum, count), value| {
                *count += 1;
                match sum {
                    Some(sum) => *sum += get_value(&value),
                    None => *sum = Some(get_value(&value)),
                }
            },
            |(sum, count), (local_sum, local_count)| {
                *count += local_count;
                match sum {
                    None => *sum = local_sum,
                    Some(sum) => {
                        if let Some(local_sum) = local_sum {
                            *sum += local_sum;
                        }
                    }
                }
            },
        )
        .map(|(_, (sum, count))| sum.unwrap() / (count as f64))
    }

    /// Count, for each partition of the stream, the number of items.
    ///
    /// The stream is partitioned using the `keyer` function.
    ///
    /// This operation is associative, therefore the computation is done in parallel before sending
    /// all the elements to the network.
    ///
    /// **Note**: this is similar to the SQL: `SELECT COUNT(*) ... GROUP BY key`
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_count(|&n| n % 2)
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_by_key(|(k, _)| *k);
    /// assert_eq!(res, vec![(0, 3), (1, 2)]);
    /// ```
    pub fn group_by_count<K, Fk>(self, keyer: Fk) -> KeyedStream<impl Operator<Out = (K, usize)>>
    where
        Fk: KeyerFn<K, Op::Out> + Fn(&Op::Out) -> K,
        K: ExchangeDataKey,
    {
        self.group_by_fold(
            keyer,
            0,
            move |count, _| *count += 1,
            |count, local_count| *count += local_count,
        )
    }

    /// Find, for each partition of the stream, the item with the smallest value.
    ///
    /// The stream is partitioned using the `keyer` function and the value to compare is obtained
    /// with `get_value`.
    ///
    /// This operation is associative, therefore the computation is done in parallel before sending
    /// all the elements to the network.
    ///
    /// **Note**: the comparison is done using the value returned by `get_value`, but the resulting
    /// items have the same type as the input.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_min_element(|&n| n % 2, |&n| n)
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (1, 1)]);
    /// ```
    pub fn group_by_min_element<K, V, Fk, Fv>(
        self,
        keyer: Fk,
        get_value: Fv,
    ) -> KeyedStream<impl Operator<Out = (K, I)>>
    where
        Fk: KeyerFn<K, Op::Out> + Fn(&Op::Out) -> K,
        Fv: KeyerFn<V, Op::Out> + Fn(&Op::Out) -> V,
        K: ExchangeDataKey,
        V: Ord,
    {
        self.group_by_reduce(keyer, move |out, value| {
            if get_value(&value) < get_value(out) {
                *out = value;
            }
        })
    }

    /// Perform the reduction operation separately for each key.
    ///
    /// This is equivalent of partitioning the stream using the `keyer` function, and then applying
    /// [`Stream::reduce_assoc`] to each partition separately.
    ///
    /// Note however that there is a difference between `stream.group_by(keyer).reduce(...)` and
    /// `stream.group_by_reduce(keyer, ...)`. The first performs the network shuffle of every item in
    /// the stream, and **later** performs the reduction (i.e. nearly all the elements will be sent to
    /// the network). The latter avoids sending the items by performing first a local reduction on
    /// each host, and then send only the locally reduced results (i.e. one message per replica, per
    /// key); then the global step is performed aggregating the results.
    ///
    /// The resulting stream will still be keyed and will contain only a single message per key (the
    /// final result).
    ///
    /// Note that the output type must be the same as the input type, if you need a different type
    /// consider using [`Stream::group_by_fold`].
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_reduce(|&n| n % 2, |acc, value| *acc += value)
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
    /// ```
    pub fn group_by_reduce<K, Fk, F>(
        self,
        keyer: Fk,
        f: F,
    ) -> KeyedStream<impl Operator<Out = (K, I)>>
    where
        Fk: Fn(&Op::Out) -> K + Send + Clone + 'static,
        F: Fn(&mut I, I) + Send + Clone + 'static,
        K: ExchangeDataKey,
    {
        let f2 = f.clone();

        self.group_by_fold(
            keyer,
            None,
            move |acc, value| match acc {
                None => *acc = Some(value),
                Some(acc) => f(acc, value),
            },
            move |acc1, acc2| match acc1 {
                None => *acc1 = acc2,
                Some(acc1) => {
                    if let Some(acc2) = acc2 {
                        f2(acc1, acc2)
                    }
                }
            },
        )
        .map(|(_, value)| value.unwrap())
    }

    /// Given two streams **with timestamps** join them according to an interval centered around the
    /// timestamp of the left side.
    ///
    /// This means that an element on the left side with timestamp T will be joined to all the
    /// elements on the right with timestamp Q such that `T - lower_bound <= Q <= T + upper_bound`.
    ///
    /// **Note**: this operator is not parallelized, all the elements are sent to a single node to
    /// perform the join.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// TODO: example
    #[cfg(feature = "timestamp")]
    pub fn interval_join<I2, Op2>(
        self,
        right: Stream<Op2>,
        lower_bound: Timestamp,
        upper_bound: Timestamp,
    ) -> Stream<impl Operator<Out = (I, I2)>>
    where
        I2: ExchangeData,
        Op2: Operator<Out = I2> + 'static,
    {
        let left = self.replication(Replication::One);
        let right = right.replication(Replication::One);
        left.merge_distinct(right)
            .key_by(|_| ())
            .add_operator(Reorder::new)
            .add_operator(|prev| IntervalJoin::new(prev, lower_bound, upper_bound))
            .drop_key()
    }

    /// Change the maximum parallelism of the following operators.
    ///
    /// **Note**: this operator is pretty advanced, some operators may need to be fully replicated
    /// and will fail otherwise.
    pub fn replication(self, replication: Replication) -> Stream<SimpleStartOperator<Op::Out>> {
        let mut new_stream = self.split_block(End::new, NextStrategy::only_one());
        new_stream
            .block
            .scheduler_requirements
            .replication(replication);
        new_stream
    }

    /// Reduce the stream into a stream that emits a single value.
    ///
    /// The reducing operator consists in adding to the current accumulation value  the value of the
    /// current item in the stream.
    ///
    /// The reducing function is provided with a mutable reference to the current accumulator and the
    /// owned item of the stream. The function should modify the accumulator without returning
    /// anything.
    ///
    /// Note that the output type must be the same as the input type, if you need a different type
    /// consider using [`Stream::fold`].
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator is not parallelized, it creates a bottleneck where all the stream
    /// elements are sent to and the folding is done using a single thread.
    ///
    /// **Note**: this is very similar to [`Iteartor::reduce`](std::iter::Iterator::reduce).
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream_iter(0..5);
    /// let res = s.reduce(|a, b| a + b).collect::<Vec<_>>();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn reduce<F>(self, f: F) -> Stream<impl Operator<Out = Op::Out>>
    where
        F: Fn(I, I) -> I + Send + Clone + 'static,
    {
        self.fold(None, move |acc, b| {
            *acc = Some(if let Some(a) = acc.take() { f(a, b) } else { b })
        })
        .map(|value| value.unwrap())
    }

    /// Reduce the stream into a stream that emits a single value.
    ///
    /// The reducing operator consists in adding to the current accumulation value the value of the
    /// current item in the stream.
    ///
    /// This method is very similary to [`Stream::reduce`], but performs the reduction distributely.
    /// To do so the reducing function must be _associative_, in particular the reducing process is
    /// performed in 2 steps:
    ///
    /// - local: the reducing function is used to reduce the elements present in each replica of
    ///   the stream independently.
    /// - global: all the partial results (the elements produced by the local step) have to be
    ///   aggregated into a single result.
    ///
    /// Note that the output type must be the same as the input type, if you need a different type
    /// consider using [`Stream::fold_assoc`].
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.reduce_assoc(|a, b| a + b).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn reduce_assoc<F>(self, f: F) -> Stream<impl Operator<Out = Op::Out>>
    where
        F: Fn(I, I) -> I + Send + Clone + 'static,
    {
        let f2 = f.clone();

        self.fold_assoc(
            None,
            move |acc, b| *acc = Some(if let Some(a) = acc.take() { f(a, b) } else { b }),
            move |acc1, mut acc2| {
                *acc1 = match (acc1.take(), acc2.take()) {
                    (Some(a), Some(b)) => Some(f2(a, b)),
                    (None, Some(a)) | (Some(a), None) => Some(a),
                    (None, None) => None,
                }
            },
        )
        .map(|value| value.unwrap())
    }

    pub fn reduce_batch<F>(self, f: F, batch_size: usize) -> Stream<impl Operator<Out = I>>
    where
        F: Fn(I, Vec<I>) -> I + Send + Clone + 'static,
    {
        self.fold_batch(
            None,
            move |acc, b| {
                *acc = Some(if let Some(a) = acc.take() {
                    f(a, b)
                } else {
                    let mut queue = VecDeque::from(b);
                    let start = queue.pop_front().unwrap();
                    f(start, queue.into())
                })
            },
            batch_size,
        )
        .map(|value| value.unwrap())
    }

    pub fn reduce_batch_assoc<F>(
        self,
        f: F,
        local_batch_size: usize,
        global_batch_size: usize,
    ) -> Stream<impl Operator<Out = I>>
    where
        F: Fn(I, Vec<I>) -> I + Send + Clone + 'static,
    {
        let f2 = f.clone();

        self.fold_batch_assoc(
            None,
            move |acc, b| {
                *acc = Some(if let Some(a) = acc.take() {
                    f(a, b)
                } else {
                    let mut queue = VecDeque::from(b);
                    let start = queue.pop_front().unwrap();
                    f(start, queue.into())
                })
            },
            move |acc1, acc2| {
                let mut vec2 = Vec::with_capacity(global_batch_size);
                acc2.into_iter().for_each(|a| vec2.push(a.unwrap()));
                *acc1 = Some(if let Some(a) = acc1.take() {
                    f2(a, vec2)
                } else {
                    let mut queue = VecDeque::from(vec2);
                    let start = queue.pop_front().unwrap();
                    f2(start, queue.into())
                })
            },
            local_batch_size,
            global_batch_size,
        )
        .map(|value| value.unwrap())
    }

    /// Route each element depending on its content.
    ///
    /// + Routes are created with the `add_route` method, a new stream is created for each route.
    /// + Each element is routed to the first stream for which the routing condition evaluates to true.
    /// + If no route condition is satisfied, the element is dropped
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::prelude::*;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// # let s = env.stream_iter(0..10);
    /// let mut routes = s.route()
    ///     .add_route(|&i| i < 5)
    ///     .add_route(|&i| i % 2 == 0)
    ///     .build()
    ///     .into_iter();
    /// assert_eq!(routes.len(), 2);
    /// // 0 1 2 3 4
    /// routes.next().unwrap().for_each(|i| eprintln!("route1: {i}"));
    /// // 6 8
    /// routes.next().unwrap().for_each(|i| eprintln!("route2: {i}"));
    /// // 5 7 9 ignored
    /// env.execute_blocking();
    /// ```
    pub fn route(self) -> RouterBuilder<I, Op> {
        RouterBuilder::new(self)
    }

    /// Perform a network shuffle sending the messages to a random replica.
    ///
    /// This can be useful if for some reason the load is very unbalanced (e.g. after a very
    /// unbalanced [`Stream::group_by`]).
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.shuffle();
    /// ```
    pub fn shuffle(self) -> Stream<impl Operator<Out = Op::Out>> {
        self.split_block(End::new, NextStrategy::random())
    }

    /// Split the stream into `splits` streams, each with all the elements of the first one.
    ///
    /// This will effectively duplicate every item in the stream into the newly created streams.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let mut splits = s.split(3);
    /// let a = splits.pop().unwrap();
    /// let b = splits.pop().unwrap();
    /// let c = splits.pop().unwrap();
    /// ```
    pub fn split(self, splits: usize) -> Vec<Stream<impl Operator<Out = Op::Out>>> {
        // This is needed to maintain the same parallelism of the split block
        let scheduler_requirements = self.block.scheduler_requirements.clone();
        let mut new_stream = self.split_block(End::new, NextStrategy::only_one());
        new_stream.block.scheduler_requirements = scheduler_requirements;

        let mut streams = Vec::with_capacity(splits);
        for _ in 0..splits - 1 {
            streams.push(new_stream.clone());
        }
        streams.push(new_stream);

        streams
    }

    /// Given two [`Stream`]s, zip their elements together: the resulting stream will be a stream of
    /// pairs, each of which is an element from both streams respectively.
    ///
    /// **Note**: all the elements after the end of one of the streams are discarded (i.e. the
    /// resulting stream will have a number of elements that is the minimum between the lengths of
    /// the two input streams).
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s1 = env.stream(IteratorSource::new(vec!['A', 'B', 'C', 'D'].into_iter()));
    /// let s2 = env.stream(IteratorSource::new(vec![1, 2, 3].into_iter()));
    /// let res = s1.zip(s2).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![('A', 1), ('B', 2), ('C', 3)]);
    /// ```
    pub fn zip<I2, Op2>(self, oth: Stream<Op2>) -> Stream<impl Operator<Out = (I, I2)>>
    where
        Op2: Operator<Out = I2> + 'static,
        I2: ExchangeData,
    {
        let mut new_stream = self.binary_connection(
            oth,
            Zip::new,
            NextStrategy::only_one(),
            NextStrategy::only_one(),
        );
        // if the zip operator is partitioned there could be some loss of data
        new_stream
            .block
            .scheduler_requirements
            .replication(Replication::One);
        new_stream
    }

    /// Close the stream and send resulting items to a channel on a single host.
    ///
    /// If the stream is distributed among multiple replicas, parallelism will
    /// be set to 1 to gather all results
    ///
    /// **Note**: the order of items and keys is unspecified.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10u32)));
    /// let rx = s.collect_channel();
    ///
    /// env.execute_blocking();
    /// let mut v = Vec::new();
    /// while let Ok(x) = rx.recv() {
    ///     v.push(x)
    /// }
    /// assert_eq!(v, (0..10u32).collect::<Vec<_>>());
    /// ```
    pub fn collect_channel(self) -> Receiver<I> {
        let (tx, rx) = unbounded();
        self.replication(Replication::One)
            .add_operator(|prev| CollectChannelSink::new(prev, tx))
            .finalize_block();
        rx
    }
    /// Close the stream and send resulting items to a channel on each single host.
    ///
    /// Each host sends its outputs to the channel without repartitioning.
    /// Elements will be sent to the channel on the same host that produced
    /// the output.
    ///
    /// **Note**: the order of items and keys is unspecified.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10u32)));
    /// let rx = s.collect_channel();
    ///
    /// env.execute_blocking();
    /// let mut v = Vec::new();
    /// while let Ok(x) = rx.recv() {
    ///     v.push(x)
    /// }
    /// assert_eq!(v, (0..10u32).collect::<Vec<_>>());
    /// ```
    pub fn collect_channel_parallel(self) -> Receiver<I> {
        let (tx, rx) = unbounded();
        self.add_operator(|prev| CollectChannelSink::new(prev, tx))
            .finalize_block();
        rx
    }

    /// Close the stream and store all the resulting items into a [`Vec`] on a single host.
    ///
    /// If the stream is distributed among multiple replicas, a bottleneck is placed where all the
    /// replicas sends the items to.
    ///
    /// **Note**: the order of items and keys is unspecified.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// let res = s.collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), (0..10).collect::<Vec<_>>());
    /// ```
    pub fn collect_count(self) -> StreamOutput<usize> {
        let output = StreamOutputRef::default();
        self.add_operator(|prev| Fold::new(prev, 0, |acc, _| *acc += 1))
            .replication(Replication::One)
            .add_operator(|prev| CollectCountSink::new(prev, output.clone()))
            .finalize_block();
        StreamOutput::from(output)
    }

    /// Close the stream and store all the resulting items into a [`Vec`] on a single host.
    ///
    /// If the stream is distributed among multiple replicas, a bottleneck is placed where all the
    /// replicas sends the items to.
    ///
    /// **Note**: the order of items and keys is unspecified.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// let res = s.collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), (0..10).collect::<Vec<_>>());
    /// ```
    pub fn collect_vec(self) -> StreamOutput<Vec<I>> {
        let output = StreamOutputRef::default();
        self.replication(Replication::One)
            .add_operator(|prev| CollectVecSink::new(prev, output.clone()))
            .finalize_block();
        StreamOutput::from(output)
    }

    /// Close the stream and store all the resulting items into a collection on a single host.
    ///
    /// If the stream is distributed among multiple replicas, parallelism will
    /// be set to 1 to gather all results
    ///
    /// **Note**: the order of items and keys is unspecified.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// let res = s.collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), (0..10).collect::<Vec<_>>());
    /// ```
    pub fn collect<C: FromIterator<I> + Send + 'static>(self) -> StreamOutput<C> {
        let output = StreamOutputRef::default();
        self.replication(Replication::One)
            .add_operator(|prev| Collect::new(prev, output.clone()))
            .finalize_block();
        StreamOutput::from(output)
    }

    /// Closes the stream, preserving all resulting items in a [`StreamCache`]. This enables the
    /// stream to be resumed from the last processed element.
    ///
    /// **Note**: To resume the stream later, ensure that the stream is executed using
    /// [`StreamEnvironment::interactive_execute_blocking`]. Failing to do so will consume the environment.
    ///
    /// **Note**: Calling [`StreamCache::read`] on the returned cache before the execution of the stream
    /// will result in a panic.
    ///
    /// ## Example
    ///
    /// ```
    /// let mut env = StreamEnvironment::default();
    /// let source = IteratorSource::new((0..10));
    ///
    /// // Create a cached stream by applying filters and caching the results
    /// let cache = env.stream(source).filter(|x| x % 3 == 0).cache();
    ///
    /// // Execute the stream interactively to capture and store the results in the cache
    /// env.interactive_execute_blocking();
    ///
    /// // Further process the cached stream, applying additional filters and collecting the results
    /// cache.stream().filter(|x| x % 2 == 0).collect_vec();
    ///
    /// // Execute the environment to finalize the processing
    /// env.execute_blocking();
    ///
    /// // Assert the final result matches the expected values
    /// assert_eq!(res.get().unwrap(), (0..10).filter(|x| x % 3 == 0 && x % 2 == 0).collect());
    /// ```
    pub fn cache(self) -> StreamCache<I> {
        let output = StreamCacheRef::default();
        let env = self.env.clone();
        self.add_operator(|prev| CacheSink::new(prev, output.clone()))
            .finalize_block();
        StreamCache::new(output, env)
    }

    /// Create a checkpoint in a stream by caching all the elements in a [`StreamCache`].
    /// The function returns a tuple containing the cache and the stream on which operations
    /// following the checkpoint can be applied.
    ///
    /// **Note**: This operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// let mut env = StreamEnvironment::default();
    /// let source = IteratorSource::new((0..10));
    ///
    /// // Create a cached stream by applying filters and caching the results
    /// let (cache, stream) = env.stream(source).filter(|x| x % 3 == 0).checkpoint();
    ///
    /// // Further process the cached stream, applying additional filters and collecting the results
    /// let result = stream.filter(|x| x % 2 == 0).collect_vec();
    ///
    /// // Execute the environment to finalize the processing
    /// env.execute_blocking();
    ///
    /// // Assert the final result matches the expected values
    /// assert_eq!(result.get().unwrap(), (0..10).filter(|x| x % 3 == 0 && x % 2 == 0).collect());
    ///
    /// // Assert the cache contains the expected values
    /// assert_eq!(cache.read(), (0..10).filter(|x| x % 3 == 0).collect());
    /// ```
    pub fn checkpoint(self) -> (StreamCache<I>, Stream<impl Operator<Out = Op::Out>>) {
        let mut splits = self.split(2);
        (splits.pop().unwrap().cache(), splits.pop().unwrap())
    }
}

impl<Op> Stream<Op>
where
    Op: Operator + 'static,
    Op::Out: Clone + Hash + Eq + Sync,
{
    /// Map the elements of the stream into new elements by evaluating a future for each one.
    /// Use memoization to cache outputs for previously seen inputs.
    ///
    /// The cache is implemented through a *per-process* [`quick_cache::sync::Cache`].
    /// The maximum number of elements to be cached is passed as the `capacity` parameter.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # tokio::runtime::Runtime::new()
    /// #    .unwrap()
    /// #    .block_on(base());
    /// # async fn base() {
    /// #    let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream_iter((0..4).cycle().take(10));
    /// let res = s.map_async_memo(|n| async move {n * n}, 100).collect_vec();
    /// env.execute().await;
    /// assert_eq!(res.get().unwrap(), vec![0, 1, 4, 9, 0, 1, 4, 9, 0, 1]);
    /// # }
    /// ```
    #[cfg(feature = "async-tokio")]
    pub fn map_async_memo<O: Clone + Send + Sync + 'static, F, Fut>(
        self,
        f: F,
        capacity: usize,
    ) -> Stream<impl Operator<Out = O>>
    where
        F: Fn(Op::Out) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = O> + Send,
    {
        self.map_async_memo_by(f, |x: &Op::Out| x.clone(), capacity)
    }
}

impl<Op> Stream<Op>
where
    Op::Out: Clone + Hash + Eq + Sync,
    Op: Operator + 'static,
{
    /// Map the elements of the stream into new elements. Use memoization
    /// to cache outputs for previously seen inputs.
    ///
    /// The cache is implemented through a *per-process* [`quick_cache::sync::Cache`].
    /// The maximum number of elements to be cached is passed as the `capacity` parameter.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream_iter((0..4).cycle().take(10));
    /// let res = s.map_memo(|n| n * n, 5).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 1, 4, 9, 0, 1, 4, 9, 0, 1]);
    /// ```

    pub fn map_memo<O: Data + Sync, F>(
        self,
        f: F,
        capacity: usize,
    ) -> Stream<impl Operator<Out = O>>
    where
        F: Fn(Op::Out) -> O + Send + Clone + 'static,
    {
        self.add_operator(|prev| MapMemo::new(prev, f, |x| x.clone(), capacity))
    }
}

impl<Op, K, I> KeyedStream<Op>
where
    K: DataKey,
    I: Send + 'static,
    Op: Operator<Out = (K, I)> + 'static,
{
    /// Given a keyed stream without timestamps nor watermarks, tag each item with a timestamp and insert
    /// watermarks.
    ///
    /// The two functions given to this operator are the following:
    /// - `timestamp_gen` returns the timestamp assigned to the provided element of the stream
    /// - `watermark_gen` returns an optional watermark to add after the provided element
    ///
    /// Note that the two functions **must** follow the watermark semantics.
    /// TODO: link to watermark semantics
    ///
    /// ## Example
    ///
    /// In this example the stream contains the integers from 0 to 9 and group them by parity, each will be tagged with a
    /// timestamp with the value of the item as milliseconds, and after each even number a watermark
    /// will be inserted.
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// use noir_compute::operator::Timestamp;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    ///
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// s
    ///     .group_by(|i| i % 2)
    ///     .add_timestamps(
    ///     |&(_k, n)| n,
    ///     |&(_k, n), &ts| if n % 2 == 0 { Some(ts) } else { None }
    /// );
    /// ```
    #[cfg(feature = "timestamp")]
    pub fn add_timestamps<F, G>(
        self,
        timestamp_gen: F,
        watermark_gen: G,
    ) -> KeyedStream<impl Operator<Out = Op::Out>>
    where
        F: FnMut(&Op::Out) -> Timestamp + Clone + Send + 'static,
        G: FnMut(&Op::Out, &Timestamp) -> Option<Timestamp> + Clone + Send + 'static,
    {
        self.add_operator(|prev| AddTimestamp::new(prev, timestamp_gen, watermark_gen))
    }

    #[cfg(feature = "timestamp")]
    pub fn drop_timestamps(self) -> KeyedStream<impl Operator<Out = Op::Out>> {
        self.add_operator(|prev| DropTimestamp::new(prev))
    }

    /// Change the batch mode for this stream.
    ///
    /// This change will be propagated to all the operators following, even of the next blocks,
    /// until it's changed again.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// use noir_compute::BatchMode;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    ///
    /// let s = env.stream(IteratorSource::new((0..10))).group_by(|&n| n % 2);
    /// s.batch_mode(BatchMode::fixed(1024));
    /// ```
    pub fn batch_mode(mut self, batch_mode: BatchMode) -> Self {
        self.0.block.batch_mode = batch_mode;
        self
    }

    /// Remove from the stream all the elements for which the provided function returns `None` and
    /// keep the elements that returned `Some(_)`.
    ///
    /// **Note**: this is very similar to [`Iteartor::filter_map`](std::iter::Iterator::filter_map)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10))).group_by(|&n| n % 2);
    /// let res = s.filter_map(|(_key, n)| if n % 3 == 0 { Some(n * 4) } else { None }).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 24), (1, 12), (1, 36)]);
    /// ```
    pub fn filter_map<O, F>(self, f: F) -> KeyedStream<impl Operator<Out = (K, O)>>
    where
        F: Fn((&K, I)) -> Option<O> + Send + Clone + 'static,
        O: Send + 'static,
    {
        self.map(f)
            .filter(|(_, x)| x.is_some())
            .map(|(_, x)| x.unwrap())
    }

    /// Remove from the stream all the elements for which the provided predicate returns `false`.
    ///
    /// **Note**: this is very similar to [`Iteartor::filter`](std::iter::Iterator::filter)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10))).group_by(|&n| n % 2);
    /// let res = s.filter(|&(_key, n)| n % 3 == 0).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 6), (1, 3), (1, 9)]);
    /// ```
    pub fn filter<F>(self, predicate: F) -> KeyedStream<impl Operator<Out = (K, I)>>
    where
        F: Fn(&(K, I)) -> bool + Clone + Send + 'static,
    {
        self.add_operator(|prev| Filter::new(prev, predicate))
    }

    /// Apply a mapping operation to each element of the stream, the resulting stream will be the
    /// flatMaped values of the result of the mapping.
    ///
    /// **Note**: this is very similar to [`Iteartor::flat_map`](std::iter::Iterator::flat_map).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..3))).group_by(|&n| n % 2);
    /// let res = s.flat_map(|(_key, n)| vec![n, n]).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 0), (0, 2), (0, 2), (1, 1), (1, 1)]);
    /// ```
    pub fn flat_map<O, It, F>(self, f: F) -> KeyedStream<impl Operator<Out = (K, O)>>
    where
        It: IntoIterator<Item = O>,
        <It as IntoIterator>::IntoIter: Send + 'static,
        F: Fn(Op::Out) -> It + Send + Clone + 'static,
        O: Data,
        It: 'static,
    {
        self.add_operator(|prev| KeyedFlatMap::new(prev, f))
    }

    /// Apply the given function to all the elements of the stream, consuming the stream.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5))).group_by(|&n| n % 2);
    /// s.inspect(|(key, n)| println!("Item: {} has key {}", n, key)).for_each(std::mem::drop);
    ///
    /// env.execute_blocking();
    /// ```
    pub fn inspect<F>(self, f: F) -> KeyedStream<impl Operator<Out = (K, I)>>
    where
        F: FnMut(&(K, I)) + Send + Clone + 'static,
    {
        self.add_operator(|prev| Inspect::new(prev, f))
    }

    /// Perform the folding operation separately for each key.
    ///
    /// Note that there is a difference between `stream.group_by(keyer).fold(...)` and
    /// `stream.group_by_fold(keyer, ...)`. The first performs the network shuffle of every item in
    /// the stream, and **later** performs the folding (i.e. nearly all the elements will be sent to
    /// the network). The latter avoids sending the items by performing first a local reduction on
    /// each host, and then send only the locally folded results (i.e. one message per replica, per
    /// key); then the global step is performed aggregating the results.
    ///
    /// The resulting stream will still be keyed and will contain only a single message per key (the
    /// final result).
    ///
    /// Note that the output type may be different from the input type. Consider using
    /// [`KeyedStream::reduce`] if the output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5))).group_by(|&n| n % 2);
    /// let res = s
    ///     .fold(0, |acc, value| *acc += value)
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
    /// ```
    pub fn fold<O, F>(self, init: O, f: F) -> KeyedStream<impl Operator<Out = (K, O)>>
    where
        F: Fn(&mut O, <Op::Out as KeyedItem>::Value) + Send + Clone + 'static,
        O: Send + Clone,
    {
        self.add_operator(|prev| KeyedFold::new(prev, init, f))
    }

    /// Perform the reduction operation separately for each key.
    ///
    /// Note that there is a difference between `stream.group_by(keyer).reduce(...)` and
    /// `stream.group_by_reduce(keyer, ...)`. The first performs the network shuffle of every item in
    /// the stream, and **later** performs the reduction (i.e. nearly all the elements will be sent to
    /// the network). The latter avoids sending the items by performing first a local reduction on
    /// each host, and then send only the locally reduced results (i.e. one message per replica, per
    /// key); then the global step is performed aggregating the results.
    ///
    /// The resulting stream will still be keyed and will contain only a single message per key (the
    /// final result).
    ///
    /// Note that the output type must be the same as the input type, if you need a different type
    /// consider using [`KeyedStream::fold`].
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5))).group_by(|&n| n % 2);
    /// let res = s
    ///     .reduce(|acc, value| *acc += value)
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
    /// ```
    pub fn reduce<F>(self, f: F) -> KeyedStream<impl Operator<Out = (K, I)>>
    where
        I: Clone + 'static,
        F: Fn(&mut I, I) + Send + Clone + 'static,
    {
        self.fold(None, move |acc, value| match acc {
            None => *acc = Some(value),
            Some(acc) => f(acc, value),
        })
        .map(|(_, value)| value.unwrap())
    }

    /// Map the elements of the stream into new elements.
    ///
    /// **Note**: this is very similar to [`Iteartor::map`](std::iter::Iterator::map).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5))).group_by(|&n| n % 2);
    /// let res = s.map(|(_key, n)| 10 * n).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 20), (0, 40), (1, 10), (1, 30)]);
    /// ```
    pub fn map<O, F>(self, f: F) -> KeyedStream<impl Operator<Out = (K, O)>>
    where
        F: Fn((&K, I)) -> O + Send + Clone + 'static,
        O: Send,
    {
        self.add_operator(|prev| {
            Map::new(prev, move |(k, v)| {
                let mapped_value = f((&k, v));
                (k, mapped_value)
            })
        })
    }

    /// # TODO
    /// Reorder timestamped items
    pub fn reorder(self) -> KeyedStream<impl Operator<Out = (K, I)>> {
        self.add_operator(|prev| Reorder::new(prev))
    }

    /// Map the elements of the stream into new elements. The mapping function can be stateful.
    ///
    /// This is exactly like [`Stream::rich_map`], but the function is cloned for each key. This
    /// means that each key will have a unique mapping function (and therefore a unique state).
    pub fn rich_map<O, F>(self, f: F) -> KeyedStream<impl Operator<Out = (K, O)>>
    where
        F: FnMut((&K, I)) -> O + Clone + Send + 'static,
        O: Data,
    {
        self.add_operator(|prev| RichMap::new(prev, f))
    }

    /// Apply a mapping operation to each element of the stream, the resulting stream will be the
    /// flattened values of the result of the mapping. The mapping function can be stateful.
    ///
    /// This is exactly like [`Stream::rich_flat_map`], but the function is cloned for each key.
    /// This means that each key will have a unique mapping function (and therefore a unique state).
    pub fn rich_flat_map<O, It, F>(self, f: F) -> KeyedStream<impl Operator<Out = (K, O)>>
    where
        It: IntoIterator<Item = O>,
        <It as IntoIterator>::IntoIter: Clone + Send + 'static,
        F: FnMut((&K, I)) -> It + Clone + Send + 'static,
        O: Data,
        It: Data,
    {
        self.rich_map(f).flatten()
    }

    /// Remove from the stream all the elements for which the provided function returns `None` and
    /// keep the elements that returned `Some(_)`. The mapping function can be stateful.
    ///
    /// This is exactly like [`Stream::rich_filter_map`], but the function is cloned for each key.
    /// This means that each key will have a unique mapping function (and therefore a unique state).
    pub fn rich_filter_map<O, F>(self, f: F) -> KeyedStream<impl Operator<Out = (K, O)>>
    where
        F: FnMut((&K, I)) -> Option<O> + Send + Clone + 'static,
        O: Data,
    {
        self.rich_map(f)
            .filter(|(_, x)| x.is_some())
            .map(|(_, x)| x.unwrap())
    }

    /// Map the elements of the stream into new elements. The mapping function can be stateful.
    ///
    /// This is exactly like [`Stream::rich_map`], but the function is cloned for each key. This
    /// means that each key will have a unique mapping function (and therefore a unique state).
    pub fn rich_map_custom<O, F>(self, f: F) -> Stream<impl Operator<Out = O>>
    where
        F: FnMut(ElementGenerator<Op>) -> StreamElement<O> + Clone + Send + 'static,
        O: Data,
    {
        self.0.add_operator(|prev| RichMapCustom::new(prev, f))
    }

    /// Make this [`KeyedStream`] a normal [`Stream`] of key-value pairs.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let stream = env.stream(IteratorSource::new((0..4))).group_by(|&n| n % 2);
    /// let res = stream.unkey().collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable(); // the output order is nondeterministic
    /// assert_eq!(res, vec![(0, 0), (0, 2), (1, 1), (1, 3)]);
    /// ```
    pub fn unkey(self) -> Stream<impl Operator<Out = (K, I)>> {
        self.0
    }

    /// Forget about the key of this [`KeyedStream`] and return a [`Stream`] containing just the
    /// values.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let stream = env.stream(IteratorSource::new((0..4))).group_by(|&n| n % 2);
    /// let res = stream.drop_key().collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable(); // the output order is nondeterministic
    /// assert_eq!(res, (0..4).collect::<Vec<_>>());
    /// ```
    pub fn drop_key(self) -> Stream<impl Operator<Out = I>> {
        self.0.map(|(_k, v)| v)
    }

    /// Apply the given function to all the elements of the stream, consuming the stream.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5))).group_by(|&n| n % 2);
    /// s.for_each(|(key, n)| println!("Item: {} has key {}", n, key));
    ///
    /// env.execute_blocking();
    /// ```
    pub fn for_each<F>(self, f: F)
    where
        F: FnMut((K, I)) + Send + Clone + 'static,
    {
        self.0
            .add_operator(|prev| ForEach::new(prev, f))
            .finalize_block();
    }
}

impl<K, I, Op> KeyedStream<Op>
where
    Op: Operator<Out = (K, I)> + 'static,
    K: ExchangeDataKey,
    I: ExchangeData,
{
    /// Given two streams **with timestamps** join them according to an interval centered around the
    /// timestamp of the left side.
    ///
    /// This means that an element on the left side with timestamp T will be joined to all the
    /// elements on the right with timestamp Q such that `T - lower_bound <= Q <= T + upper_bound`.
    /// Only items with the same key can be joined together.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// TODO: example
    #[cfg(feature = "timestamp")]
    pub fn interval_join<I2, Op2>(
        self,
        right: KeyedStream<Op2>,
        lower_bound: Timestamp,
        upper_bound: Timestamp,
    ) -> KeyedStream<impl Operator<Out = (K, (I, I2))>>
    where
        I2: ExchangeData,
        Op2: Operator<Out = (K, I2)> + 'static,
    {
        self.merge_distinct(right)
            .add_operator(Reorder::new)
            .add_operator(|prev| IntervalJoin::new(prev, lower_bound, upper_bound))
    }

    /// Merge the items of this stream with the items of another stream with the same type.
    ///
    /// **Note**: the order of the resulting items is not specified.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s1 = env.stream(IteratorSource::new((0..3))).group_by(|&n| n % 2);
    /// let s2 = env.stream(IteratorSource::new((3..5))).group_by(|&n| n % 2);
    /// let res = s1.merge(s2).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable(); // the output order is nondeterministic
    /// assert_eq!(res, vec![(0, 0), (0, 2), (0, 4), (1, 1), (1, 3)]);
    /// ```
    pub fn merge<Op2>(self, oth: KeyedStream<Op2>) -> KeyedStream<impl Operator<Out = (K, I)>>
    where
        Op2: Operator<Out = (K, I)> + 'static,
    {
        KeyedStream(self.0.merge(oth.0))
    }

    pub(crate) fn merge_distinct<I2, Op2>(
        self,
        right: KeyedStream<Op2>,
    ) -> KeyedStream<impl Operator<Out = (K, MergeElement<I, I2>)>>
    where
        I2: ExchangeData,
        Op2: Operator<Out = (K, I2)> + 'static,
    {
        // map the left and right streams to the same type
        let left = self.map(|(_, x)| MergeElement::Left(x));
        let right = right.map(|(_, x)| MergeElement::Right(x));

        left.merge(right)
    }

    /// Perform a network shuffle sending the messages to a random replica.
    ///
    /// This operator returns a `Stream` instead of a `KeyedStream` as after
    /// shuffling the messages between replicas, the keyed semantics are lost.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.shuffle();
    /// ```

    pub fn shuffle(self) -> Stream<impl Operator<Out = Op::Out>> {
        self.0.split_block(End::new, NextStrategy::random())
    }

    /// Close the stream and send resulting items to a channel on a single host.
    ///
    /// If the stream is distributed among multiple replicas, parallelism will
    /// be set to 1 to gather all results
    ///
    /// **Note**: the order of items and keys is unspecified.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10u32)));
    /// let rx = s.collect_channel();
    ///
    /// env.execute_blocking();
    /// let mut v = Vec::new();
    /// while let Ok(x) = rx.recv() {
    ///     v.push(x)
    /// }
    /// assert_eq!(v, (0..10u32).collect::<Vec<_>>());
    /// ```
    pub fn collect_channel(self) -> Receiver<(K, I)> {
        self.unkey().collect_channel()
    }
    /// Close the stream and send resulting items to a channel on each single host.
    ///
    /// Each host sends its outputs to the channel without repartitioning.
    /// Elements will be sent to the channel on the same host that produced
    /// the output.
    ///
    /// **Note**: the order of items and keys is unspecified.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10u32)));
    /// let rx = s.collect_channel();
    ///
    /// env.execute_blocking();
    /// let mut v = Vec::new();
    /// while let Ok(x) = rx.recv() {
    ///     v.push(x)
    /// }
    /// assert_eq!(v, (0..10u32).collect::<Vec<_>>());
    /// ```
    pub fn collect_channel_parallel(self) -> Receiver<(K, I)> {
        self.unkey().collect_channel_parallel()
    }

    /// Close the stream and store all the resulting items into a [`Vec`] on a single host.
    ///
    /// If the stream is distributed among multiple replicas, a bottleneck is placed where all the
    /// replicas sends the items to.
    ///
    /// **Note**: the collected items are the pairs `(key, value)`.
    ///
    /// **Note**: the order of items and keys is unspecified.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..3))).group_by(|&n| n % 2);
    /// let res = s.collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable(); // the output order is nondeterministic
    /// assert_eq!(res, vec![(0, 0), (0, 2), (1, 1)]);
    /// ```
    pub fn collect_vec(self) -> StreamOutput<Vec<(K, I)>> {
        self.unkey().collect_vec()
    }

    /// Close the stream and store all the resulting items into a collection on a single host.
    ///
    /// If the stream is distributed among multiple replicas, parallelism will
    /// be set to 1 to gather all results
    ///
    ///
    /// **Note**: the order of items and keys is unspecified.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..3))).group_by(|&n| n % 2);
    /// let res = s.collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable(); // the output order is nondeterministic
    /// assert_eq!(res, vec![(0, 0), (0, 2), (1, 1)]);
    /// ```
    pub fn collect<C: FromIterator<(K, I)> + Send + 'static>(self) -> StreamOutput<C> {
        self.unkey().collect()
    }
}

impl<K, I, O, It, Op> KeyedStream<Op>
where
    K: DataKey,
    Op: Operator<Out = (K, I)> + 'static,
    It: Iterator<Item = O> + Clone + Send + 'static,
    I: Data + IntoIterator<IntoIter = It, Item = It::Item>,
    O: Data + Clone,
    K: DataKey,
{
    /// Transform this stream of containers into a stream of all the contained values.
    ///
    /// **Note**: this is very similar to [`Iteartor::flatten`](std::iter::Iterator::flatten)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir_compute::{StreamEnvironment, EnvironmentConfig};
    /// # use noir_compute::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env
    ///     .stream(IteratorSource::new(vec![
    ///         vec![0, 1, 2],
    ///         vec![3, 4, 5],
    ///         vec![6, 7]
    ///     ].into_iter()))
    ///     .group_by(|v| v[0] % 2);
    /// let res = s.flatten().collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 1), (0, 2), (0, 6), (0, 7), (1, 3), (1, 4), (1, 5)]);
    /// ```
    pub fn flatten(self) -> KeyedStream<impl Operator<Out = (K, O)>> {
        self.add_operator(|prev| KeyedFlatten::new(prev))
    }
}
