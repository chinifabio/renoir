use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::KeyedItem;

#[derive(Debug)]
pub struct RichMap<K, I, O, F, OperatorChain>
where
    F: FnMut(
            (
                &<OperatorChain::Out as KeyedItem>::Key,
                <OperatorChain::Out as KeyedItem>::Value,
            ),
        ) -> O
        + Clone
        + Send,
    OperatorChain: Operator,
    OperatorChain::Out: KeyedItem<Key = K, Value = I>,
{
    prev: OperatorChain,
    maps_fn: HashMap<K, F, crate::block::GroupHasherBuilder>,
    init_map: F,
    _i: PhantomData<I>,
    _o: PhantomData<O>,
}

impl<K: DataKey, I, O, F: Clone, OperatorChain: Clone> Clone for RichMap<K, I, O, F, OperatorChain>
where
    F: FnMut(
            (
                &<OperatorChain::Out as KeyedItem>::Key,
                <OperatorChain::Out as KeyedItem>::Value,
            ),
        ) -> O
        + Clone
        + Send,
    OperatorChain: Operator,
    OperatorChain::Out: KeyedItem<Key = K, Value = I>,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            maps_fn: self.maps_fn.clone(),
            init_map: self.init_map.clone(),
            _i: self._i,
            _o: self._o,
        }
    }
}

impl<K: DataKey, I: Send, O: Send, F, OperatorChain> Display for RichMap<K, I, O, F, OperatorChain>
where
    F: FnMut(
            (
                &<OperatorChain::Out as KeyedItem>::Key,
                <OperatorChain::Out as KeyedItem>::Value,
            ),
        ) -> O
        + Clone
        + Send,
    OperatorChain: Operator,
    OperatorChain::Out: KeyedItem<Key = K, Value = I>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> RichMap<{} -> {}>",
            self.prev,
            std::any::type_name::<I>(),
            std::any::type_name::<O>()
        )
    }
}

impl<K: DataKey, I: Send, O: Send, F, OperatorChain> RichMap<K, I, O, F, OperatorChain>
where
    F: FnMut(
            (
                &<OperatorChain::Out as KeyedItem>::Key,
                <OperatorChain::Out as KeyedItem>::Value,
            ),
        ) -> O
        + Clone
        + Send,
    OperatorChain: Operator,
    OperatorChain::Out: KeyedItem<Key = K, Value = I>,
{
    pub(super) fn new(prev: OperatorChain, f: F) -> Self {
        Self {
            prev,
            maps_fn: Default::default(),
            init_map: f,
            _i: Default::default(),
            _o: Default::default(),
        }
    }
}

impl<K: DataKey, I: Send, O: Send, F, OperatorChain> Operator for RichMap<K, I, O, F, OperatorChain>
where
    K: DataKey,
    I: Send,
    O: Send,
    F: FnMut(
            (
                &<OperatorChain::Out as KeyedItem>::Key,
                <OperatorChain::Out as KeyedItem>::Value,
            ),
        ) -> O
        + Clone
        + Send,
    OperatorChain: Operator,
    OperatorChain::Out: KeyedItem<Key = K, Value = I>,
{
    type Out = (K, O);

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<(K, O)> {
        let element = self.prev.next();
        if matches!(element, StreamElement::FlushAndRestart) {
            // self.maps_fn.clear();
        }
        element.map(|item| {
            let (key, value) = item.into_kv();
            let map_fn = if let Some(map_fn) = self.maps_fn.get_mut(&key) {
                map_fn
            } else {
                // the key is not present in the hashmap, so this always inserts a new map function
                let map_fn = self.init_map.clone();
                self.maps_fn.entry(key.clone()).or_insert(map_fn)
            };

            let new_value = (map_fn)((&key, value));
            (key, new_value)
        })
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<O, _>("RichMap"))
    }
}
