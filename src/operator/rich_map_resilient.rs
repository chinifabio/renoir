use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;

use serde::Serialize;

use crate::block::{BlockStructure, OperatorStructure};
use crate::checkpointing::CheckpointManagerRef;
use crate::network::Coord;
use crate::operator::{DataKey, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;

pub trait CheckpointedFn<K, I, O>: Clone + Send {
    fn process(&mut self, input: I) -> O;
    fn process_keyed(&mut self, input: (&K, I)) -> O {
        self.process(input.1)
    }
    fn snapshot(&self) -> Vec<u8>;
    fn restore(&mut self, snapshot: Vec<u8>);
}

/// Blanket implementation: Treat all keyed FnMut closures as "stateless" operators
impl<K, I, O, F> CheckpointedFn<K, I, O> for F 
where F: FnMut((&K, I)) -> O + Clone + Send
{
    fn process(&mut self, _input: I) -> O {
        panic!("This is a keyless operation");
    }

    fn process_keyed(&mut self, input: (&K, I)) -> O {
        (self)(input)
    }

    fn snapshot(&self) -> Vec<u8> {
        Vec::new()
    }

    fn restore(&mut self, _snapshot: Vec<u8>) {
        // No state to restore
    }
}

#[derive(Debug)]
pub struct RichMapResilient<K, I, O, F, OperatorChain>
where
    F: CheckpointedFn<K, I, O>,
    OperatorChain: Operator<Out = (K, I)>,
{
    prev: OperatorChain,
    maps_fn: HashMap<K, F, crate::block::GroupHasherBuilder>,
    init_map: F,
    last_checkpoint_ts: Option<Timestamp>,
    checkpoint_manager: Option<CheckpointManagerRef>,
    coord: Option<Coord>,
    _i: PhantomData<I>,
    _o: PhantomData<O>,
}

impl<K: DataKey, I, O, F: Clone, OperatorChain: Clone> Clone for RichMapResilient<K, I, O, F, OperatorChain>
where
    F: CheckpointedFn<K, I, O>,
    OperatorChain: Operator<Out = (K, I)>,
{
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            maps_fn: self.maps_fn.clone(),
            init_map: self.init_map.clone(),
            last_checkpoint_ts: self.last_checkpoint_ts,
            checkpoint_manager: self.checkpoint_manager.clone(),
            coord: self.coord,
            _i: self._i,
            _o: self._o,
        }
    }
}

impl<K: DataKey, I: Send, O: Send, F, OperatorChain> Display for RichMapResilient<K, I, O, F, OperatorChain>
where
    F: CheckpointedFn<K, I, O>,
    OperatorChain: Operator<Out = (K, I)>,
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

impl<K: DataKey, I: Send, O: Send, F, OperatorChain> RichMapResilient<K, I, O, F, OperatorChain>
where
    F: CheckpointedFn<K, I, O>,
    OperatorChain: Operator<Out = (K, I)>,
{
    pub(super) fn new(prev: OperatorChain, f: F) -> Self {
        Self {
            prev,
            maps_fn: Default::default(),
            init_map: f,
            last_checkpoint_ts: None,
            checkpoint_manager: Default::default(),
            coord: Default::default(),
            _i: Default::default(),
            _o: Default::default(),
        }
    }
}

impl<K: DataKey + Serialize, I: Send, O: Send, F, OperatorChain> Operator for RichMapResilient<K, I, O, F, OperatorChain>
where
    K: DataKey,
    I: Send,
    O: Send,
    F: CheckpointedFn<K, I, O>,
    OperatorChain: Operator<Out = (K, I)>,
{
    type Out = (K, O);

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
        if metadata.checkpoint_manager.is_none() {
            panic!("CheckpointManager is required for RichMapResilient operator");
        }
        self.checkpoint_manager = metadata.checkpoint_manager.clone();
        self.coord = Some(metadata.coord);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<(K, O)> {
        let element = self.prev.next();
        if matches!(element, StreamElement::FlushAndRestart) {
            // self.maps_fn.clear();
        }
        if let StreamElement::Watermark(t) = element {
            let do_checkpoint = self.last_checkpoint_ts.map_or(true, |last_ts| t > last_ts);
            if do_checkpoint {
                log::info!("Checkpointing at timestamp {}", t);
                // Collect state snapshots from all map functions
                let state = self.maps_fn.iter()
                    .map(|(key, map_fn)| {
                        let snapshot = map_fn.snapshot();
                        (key, snapshot)
                    })
                    .collect::<Vec<_>>();
                
                // Serialize the collection of states
                let serialized = bincode::serde::encode_to_vec(&state, bincode::config::standard())
                    .expect("Failed to serialize checkpoint state");
                
                // Store via checkpoint manager
                self.checkpoint_manager.as_ref().unwrap().lock()
                    .checkpoint(self.coord.unwrap(), serialized);
                
                self.last_checkpoint_ts = Some(t);
            }
        }
        element.map(|(key, value)| {
            let map_fn = if let Some(map_fn) = self.maps_fn.get_mut(&key) {
                map_fn
            } else {
                // the key is not present in the hashmap, so this always inserts a new map function
                let map_fn = self.init_map.clone();
                self.maps_fn.entry(key.clone()).or_insert(map_fn)
            };

            let new_value = map_fn.process_keyed((&key, value));
            (key, new_value)
        })
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<O, _>("RichMap"))
    }
}

#[derive(Debug)]
pub struct RichMapTransient<K, I, O, F, OperatorChain>
where
    F: CheckpointedFn<K, I, O>,
    OperatorChain: Operator<Out = (K, I)>,
{
    prev: OperatorChain,
    maps_fn: HashMap<K, F, crate::block::GroupHasherBuilder>,
    init_map: F,
    _i: PhantomData<I>,
    _o: PhantomData<O>,
}

impl<K: DataKey, I, O, F: Clone, OperatorChain: Clone> Clone
    for RichMapTransient<K, I, O, F, OperatorChain>
where
    F: CheckpointedFn<K, I, O> + Clone + Send,
    OperatorChain: Operator<Out = (K, I)>,
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

impl<K: DataKey, I: Send, O: Send, F, OperatorChain> Display
    for RichMapTransient<K, I, O, F, OperatorChain>
where
    F: CheckpointedFn<K, I, O> + Clone + Send,
    OperatorChain: Operator<Out = (K, I)>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> RichMapTransient<{} -> {}>",
            self.prev,
            std::any::type_name::<I>(),
            std::any::type_name::<O>()
        )
    }
}

impl<K: DataKey, I: Send, O: Send, F, OperatorChain> RichMapTransient<K, I, O, F, OperatorChain>
where
    F: CheckpointedFn<K, I, O> + Clone + Send,
    OperatorChain: Operator<Out = (K, I)>,
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

impl<K: DataKey, I: Send, O: Send, F, OperatorChain> Operator
    for RichMapTransient<K, I, O, F, OperatorChain>
where
    K: DataKey,
    I: Send,
    O: Send,
    F: CheckpointedFn<K, I, O> + Clone + Send,
    OperatorChain: Operator<Out = (K, I)>,
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
        element.map(|(key, value)| {
            let e = self.maps_fn.entry(key.clone());
            let mut e = match e {
                Entry::Occupied(occupied_entry) => occupied_entry,
                Entry::Vacant(vacant_entry) => vacant_entry.insert_entry(self.init_map.clone()),
            };

            let new_value = e.get_mut().process_keyed((&key, value));

            (key, new_value)
        })
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<O, _>("RichMapTransient"))
    }
}
