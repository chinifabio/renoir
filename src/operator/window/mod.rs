use std::collections::VecDeque;

pub use aggregator::*;
pub use description::*;

use crate::operator::{Data, DataKey, ExchangeData, Operator, StreamElement, Timestamp};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

mod aggregator;
mod description;
mod generic_operator;
mod processing_time;
mod window_manager;

/// A WindowDescription describes how a window behaves.
pub trait WindowDescription<Key: DataKey, Out: Data>: Send {
    /// The type of the window generator of this window.
    type Generator: WindowGenerator<Key, Out> + Clone + 'static;

    /// Construct a new window generator, ready to handle elements.
    fn new_generator(&self) -> Self::Generator;

    fn to_string(&self) -> String;
}

/// A WindowGenerator handles the generation of windows for a given key.
pub trait WindowGenerator<Key: DataKey, Out: Data>: Send {
    /// Handle a new element of the stream.
    fn add(&mut self, item: StreamElement<Out>);
    /// If a window is ready, return it so that it can be processed.
    fn next_window(&mut self) -> Option<Window<Key, Out>>;
    /// Close the current open window.
    /// This method is called when a `Window` is dropped after being processed.
    fn advance(&mut self);
    /// Return the buffer from which `Window` will get the elements of the window.
    fn buffer(&self) -> &VecDeque<Out>;
}

/// A window is a collection of elements and may be associated with a timestamp.
pub struct Window<'a, Key: DataKey, Out: Data> {
    /// A reference to the generator that produced this window.
    ///
    /// This will be used for fetching the window elements and for advancing the window when this
    /// is dropped.
    gen: &'a mut dyn WindowGenerator<Key, Out>,
    /// The number of elements of this window.
    size: usize,
    /// If this window contains elements with a timestamp, a timestamp for this window is built.
    timestamp: Option<Timestamp>,
}

impl<'a, Key: DataKey, Out: Data> Window<'a, Key, Out> {
    /// An iterator to the elements of the window.
    fn items(&self) -> impl Iterator<Item = &Out> {
        self.gen.buffer().iter().take(self.size)
    }
}

impl<'a, Key: DataKey, Out: Data> Drop for Window<'a, Key, Out> {
    fn drop(&mut self) {
        self.gen.advance();
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn window<WinOut: Data, WinDescr: WindowDescription<Key, WinOut>>(
        self,
        descr: WinDescr,
    ) -> KeyedWindowedStream<Key, Out, impl Operator<KeyValue<Key, Out>>, WinOut, WinDescr> {
        KeyedWindowedStream {
            inner: self,
            descr,
            _win_out: Default::default(),
        }
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn window_all<WinOut: Data, WinDescr: WindowDescription<(), WinOut>>(
        self,
        descr: WinDescr,
    ) -> WindowedStream<Out, impl Operator<KeyValue<(), Out>>, WinOut, WinDescr> {
        // max_parallelism and key_by are used instead of group_by so that there is exactly one
        // replica, since window_all cannot be parallelized
        let stream = self.max_parallelism(1).key_by(|_| ()).window(descr);
        WindowedStream { inner: stream }
    }
}
