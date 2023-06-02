use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::ops::{Add, Div};

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::Stream;

use super::{Data, Timestamp};

#[derive(Debug)]
pub struct MedianExact<Out, PreviousOperators, NewOut, F>
where
    Out: ExchangeData,
    NewOut: ExchangeData + Ord + Add<Output = NewOut> + Div<i32, Output = NewOut>,
    PreviousOperators: Operator<Out>,
    F: Fn(&Out) -> NewOut + Clone + Send + Copy + 'static,
{
    prev: PreviousOperators,
    max_heap: BinaryHeap<NewOut>,
    min_heap: BinaryHeap<Reverse<NewOut>>,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
    get_value: F,
    result: Option<NewOut>,
    _out: PhantomData<Out>,
}

impl<Out, PreviousOperators, NewOut, F> MedianExact<Out, PreviousOperators, NewOut, F>
where
    Out: ExchangeData,
    NewOut: ExchangeData + Ord + Add<Output = NewOut> + Div<i32, Output = NewOut>,
    PreviousOperators: Operator<Out>,
    F: Fn(&Out) -> NewOut + Clone + Send + Copy + 'static,
{
    pub(crate) fn new(prev: PreviousOperators, get_value: F) -> Self {
        Self {
            prev,
            max_heap: BinaryHeap::new(),
            min_heap: BinaryHeap::new(),
            timestamp: None,
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
            get_value,
            result: None,
            _out: Default::default(),
        }
    }
}

impl<Out: ExchangeData, PreviousOperators, NewOut, F> Display
    for MedianExact<Out, PreviousOperators, NewOut, F>
where
    Out: ExchangeData,
    NewOut: ExchangeData + Ord + Add<Output = NewOut> + Div<i32, Output = NewOut>,
    PreviousOperators: Operator<Out>,
    F: Fn(&Out) -> NewOut + Clone + Send + Copy + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> CollectVecSink", self.prev)
    }
}

impl<Out, PreviousOperators, NewOut, F> Operator<NewOut>
    for MedianExact<Out, PreviousOperators, NewOut, F>
where
    Out: ExchangeData,
    NewOut: ExchangeData + Ord + Add<Output = NewOut> + Div<i32, Output = NewOut>,
    PreviousOperators: Operator<Out>,
    F: Fn(&Out) -> NewOut + Clone + Send + Copy + 'static,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<NewOut> {
        while !self.received_end {
            match self.prev.next() {
                StreamElement::Item(t) => {
                    let v = (self.get_value)(&t);
                    if !self.min_heap.is_empty() && v < self.min_heap.peek().unwrap().clone().0 {
                        self.max_heap.push(v);
                        if self.max_heap.len() > self.min_heap.len() + 1 {
                            self.min_heap.push(Reverse(self.max_heap.pop().unwrap()));
                        }
                    } else {
                        self.min_heap.push(Reverse(v));
                        if self.min_heap.len() > self.max_heap.len() + 1 {
                            self.max_heap.push(self.min_heap.pop().unwrap().0);
                        }
                    }
                }
                StreamElement::Timestamped(t, ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    let v = (self.get_value)(&t);
                    if !self.min_heap.is_empty() && v < self.min_heap.peek().unwrap().clone().0 {
                        self.max_heap.push(v);
                        if self.max_heap.len() > self.min_heap.len() + 1 {
                            self.min_heap.push(Reverse(self.max_heap.pop().unwrap()));
                        }
                    } else {
                        self.min_heap.push(Reverse(v));
                        if self.min_heap.len() > self.max_heap.len() + 1 {
                            self.max_heap.push(self.min_heap.pop().unwrap().0);
                        }
                    }
                }

                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                StreamElement::Terminate => {
                    self.received_end = true;
                    match self.min_heap.len().cmp(&self.max_heap.len()) {
                        std::cmp::Ordering::Less => self.result = self.max_heap.peek().cloned(),
                        std::cmp::Ordering::Greater => {
                            self.result = Some(self.min_heap.peek().cloned().unwrap().0)
                        }
                        std::cmp::Ordering::Equal => {
                            self.result = Some(
                                (self.max_heap.peek().cloned().unwrap()
                                    + self.min_heap.peek().cloned().unwrap().0)
                                    / 2,
                            );
                        }
                    }
                }
                StreamElement::FlushBatch => {}
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    self.received_end_iter = true;
                }
            }
        }
        if let Some(median) = self.result.take() {
            if let Some(ts) = self.timestamp.take() {
                return StreamElement::Timestamped(median, ts);
            } else {
                return StreamElement::Item(median);
            }
        }

        if let Some(ts) = self.max_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        // the end was not really the end... just the end of one iteration!
        if self.received_end_iter {
            self.received_end_iter = false;
            self.received_end = false;
            return StreamElement::FlushAndRestart;
        }

        StreamElement::Terminate
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("CollectVecSink");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}

impl<Out: ExchangeData, PreviousOperators, NewOut, F> Clone
    for MedianExact<Out, PreviousOperators, NewOut, F>
where
    Out: ExchangeData,
    NewOut: ExchangeData + Ord + Add<Output = NewOut> + Div<i32, Output = NewOut>,
    PreviousOperators: Operator<Out>,
    F: Fn(&Out) -> NewOut + Clone + Send + Copy + 'static,
{
    fn clone(&self) -> Self {
        panic!("MedianExact cannot be cloned, max_parallelism should be 1");
    }
}

impl<I, Op> Stream<I, Op>
where
    I: Data,
    Op: Operator<I> + 'static,
{
    pub fn median_exact<F>(self, get_value: F) -> Stream<I, impl Operator<I>>
    where
        I: ExchangeData + Ord + Add<Output = I> + Div<i32, Output = I>,
        F: Fn(&I) -> I + Clone + Send + Copy + 'static,
    {
        self.max_parallelism(1)
            .add_operator(|prev| MedianExact::new(prev, get_value))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        operator::{median_exact::MedianExact, Operator, StreamElement},
        test::FakeOperator,
    };

    #[test]
    fn median_exact() {
        let fake_operator = FakeOperator::new(0..10);
        let mut median = MedianExact::new(fake_operator, |&v| v);

        assert_eq!(median.next(), StreamElement::Item(4));
        assert_eq!(median.next(), StreamElement::Terminate);
    }
}
