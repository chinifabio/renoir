use std::marker::PhantomData;

use crate::operator::join::ship::{JoinStreamShipBroadcastRight, JoinStreamShipHash};
use crate::operator::{Data, DataKey, ExchangeData, KeyerFn, Operator};
use crate::stream::Stream;

mod local_hash;
// mod local_sort_merge;
mod ship;
mod start;

/// Type alias for a pair of joined items in an inner join.
pub type InnerJoinTuple<Out1, Out2> = (Out1, Out2);
/// Type alias for a pair of joined items in a left join.
pub type LeftJoinTuple<Out1, Out2> = (Out1, Option<Out2>);
/// Type alias for a pair of joined items in an outer join.
pub type OuterJoinTuple<Out1, Out2> = (Option<Out1>, Option<Out2>);

/// Intermediate stream type for building the join between two streams.
///
/// This type has methods for selecting the ship strategy of the join, later you will be able to
/// select the local strategy, and finally the variant of the join.
pub struct JoinStream<
    Key,
    Out1: ExchangeData,
    Out2: ExchangeData,
    OperatorChain1,
    OperatorChain2,
    Keyer1,
    Keyer2,
> where
    OperatorChain1: Operator<Out1>,
    OperatorChain2: Operator<Out2>,
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    /// The stream of the left side.
    pub(crate) lhs: Stream<Out1, OperatorChain1>,
    /// The stream of the right side.
    pub(crate) rhs: Stream<Out2, OperatorChain2>,
    /// The function for extracting the join key from the left stream.
    pub(crate) keyer1: Keyer1,
    /// The function for extracting the join key from the right stream.
    pub(crate) keyer2: Keyer2,

    _key: PhantomData<Key>,
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    pub fn join_with<Out2: ExchangeData, OperatorChain2, Key, Keyer1, Keyer2>(
        self,
        rhs: Stream<Out2, OperatorChain2>,
        keyer1: Keyer1,
        keyer2: Keyer2,
    ) -> JoinStream<Key, Out, Out2, OperatorChain, OperatorChain2, Keyer1, Keyer2>
    where
        OperatorChain2: Operator<Out2>,
        Keyer1: KeyerFn<Key, Out>,
        Keyer2: KeyerFn<Key, Out2>,
    {
        JoinStream {
            lhs: self,
            rhs,
            keyer1,
            keyer2,
            _key: PhantomData::default(),
        }
    }
}

impl<
        Key: Data,
        Out1: ExchangeData,
        Out2: ExchangeData,
        OperatorChain1,
        OperatorChain2,
        Keyer1,
        Keyer2,
    > JoinStream<Key, Out1, Out2, OperatorChain1, OperatorChain2, Keyer1, Keyer2>
where
    OperatorChain1: Operator<Out1> + 'static,
    OperatorChain2: Operator<Out2> + 'static,
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    /// Use the Hash Repartition strategy.
    ///
    /// With this strategy the two streams are shuffled (like a group-by), pointing the message with
    /// the same key to the same replica. The key must be hashable.
    pub fn ship_hash(self) -> JoinStreamShipHash<Key, Out1, Out2, Keyer1, Keyer2>
    where
        Key: DataKey,
    {
        JoinStreamShipHash::new(self)
    }

    /// Use the Broadcast-Forward strategy.
    ///
    /// The left side won't be sent to the network, while the right side is broadcasted. This is
    /// recommended when the left side is really big and the left side really small.
    ///
    /// This does not require the key to be hashable.
    pub fn ship_broadcast_right(
        self,
    ) -> JoinStreamShipBroadcastRight<Key, Out1, Out2, Keyer1, Keyer2> {
        JoinStreamShipBroadcastRight::new(self)
    }
}
