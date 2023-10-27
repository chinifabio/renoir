use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::ops::{Add, Div};
use std::vec;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::data_type::{NoirData, NoirType};
use crate::operator::{ExchangeData, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::{Replication, Stream};

use super::{SimpleStartOperator, Timestamp};

#[derive(Debug)]
pub struct MedianExact<Out: ExchangeData, PreviousOperators, NewOut, F>
where
    NewOut: ExchangeData + Ord + Add<Output = NewOut> + Div<f32, Output = NewOut>,
    PreviousOperators: Operator<Out>,
    F: Fn(Out) -> NewOut + Clone + Send + Copy + 'static,
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
    NewOut: ExchangeData + Ord + Add<Output = NewOut> + Div<f32, Output = NewOut>,
    PreviousOperators: Operator<Out>,
    F: Fn(Out) -> NewOut + Clone + Send + Copy + 'static,
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
    NewOut: ExchangeData + Ord + Add<Output = NewOut> + Div<f32, Output = NewOut>,
    PreviousOperators: Operator<Out>,
    F: Fn(Out) -> NewOut + Clone + Send + Copy + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> Median_Exact", self.prev)
    }
}

impl<Out, PreviousOperators, NewOut, F> Operator<NewOut>
    for MedianExact<Out, PreviousOperators, NewOut, F>
where
    Out: ExchangeData,
    NewOut: ExchangeData + Ord + Add<Output = NewOut> + Div<f32, Output = NewOut>,
    PreviousOperators: Operator<Out>,
    F: Fn(Out) -> NewOut + Clone + Send + Copy + 'static,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<NewOut> {
        while !self.received_end {
            match self.prev.next() {
                StreamElement::Item(t) => {
                    let v = (self.get_value)(t);
                    if !self.min_heap.is_empty() && v < self.min_heap.peek().unwrap().0 {
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
                    let v = (self.get_value)(t);
                    if !self.min_heap.is_empty() && v < self.min_heap.peek().unwrap().0 {
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
                        std::cmp::Ordering::Less => self.result = self.max_heap.pop(),
                        std::cmp::Ordering::Greater => {
                            self.result = Some(self.min_heap.pop().unwrap().0)
                        }
                        std::cmp::Ordering::Equal => {
                            self.result = Some(
                                (self.max_heap.pop().unwrap() + self.min_heap.pop().unwrap().0)
                                    / 2.0,
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
        let mut operator = OperatorStructure::new::<Out, _>("Median_Exact");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}

impl<Out: ExchangeData, PreviousOperators, NewOut, F> Clone
    for MedianExact<Out, PreviousOperators, NewOut, F>
where
    Out: ExchangeData,
    NewOut: ExchangeData + Ord + Add<Output = NewOut> + Div<f32, Output = NewOut>,
    PreviousOperators: Operator<Out>,
    F: Fn(Out) -> NewOut + Clone + Send + Copy + 'static,
{
    fn clone(&self) -> Self {
        panic!("MedianExact cannot be cloned, max_parallelism should be 1");
    }
}

impl<D, Op> Stream<D, Op>
where
    D: ExchangeData,
    Op: Operator<D> + 'static,
{
    /// Reduce the stream to its median value.
    ///
    /// get_value is a function that returns the value to be used for the median calculation.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: since this operator compute the exact median it cannot be parallelized.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::data_type::{NoirData, NoirType};
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new([NoirType::from(0.0), NoirType::from(8.0), NoirType::from(6.0)].into_iter()));
    /// let res = s.median(|v| v).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![NoirType::from(6.0)]);
    /// ```
    pub fn median<F, I>(self, get_value: F) -> Stream<I, impl Operator<I>>
    where
        D: ExchangeData,
        I: ExchangeData + Ord + Add<Output = I> + Div<f32, Output = I>,
        F: Fn(D) -> I + Clone + Send + Copy + 'static,
    {
        self.replication(Replication::One)
            .add_operator(|prev| MedianExact::new(prev, get_value))
    }
}

#[derive(Debug)]
pub struct MedianExactNoirData<PreviousOperators>
where
    PreviousOperators: Operator<NoirData>,
{
    prev: PreviousOperators,
    quantile: f32,
    max_heaps: Option<Vec<BinaryHeap<NoirType>>>,
    min_heaps: Option<Vec<BinaryHeap<Reverse<NoirType>>>>,
    found_nan: bool,
    columns_nan: Option<Vec<bool>>,
    skip_nan: bool,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
    result: Option<NoirData>,
}

impl<PreviousOperators> MedianExactNoirData<PreviousOperators>
where
    PreviousOperators: Operator<NoirData>,
{
    pub(crate) fn new(prev: PreviousOperators, quantile: f32, skip_nan: bool) -> Self {
        Self {
            prev,
            quantile,
            max_heaps: None,
            min_heaps: None,
            timestamp: None,
            max_watermark: None,
            found_nan: false,
            columns_nan: None,
            skip_nan,
            received_end: false,
            received_end_iter: false,
            result: None,
        }
    }
}

impl<PreviousOperators> Display for MedianExactNoirData<PreviousOperators>
where
    PreviousOperators: Operator<NoirData>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> Median_Exact_Noir_Data", self.prev)
    }
}

impl<PreviousOperator> MedianExactNoirData<PreviousOperator>
where
    PreviousOperator: Operator<NoirData>,
{
    fn handle_noir_type(&mut self, item: NoirType) {
        if !self.found_nan {
            if self.max_heaps.is_none() {
                self.max_heaps = Some(vec![BinaryHeap::new()]);
            }
            if self.min_heaps.is_none() {
                self.min_heaps = Some(vec![BinaryHeap::new()]);
            }

            let max_heap = &mut self.max_heaps.as_mut().unwrap()[0];
            let min_heap = &mut self.min_heaps.as_mut().unwrap()[0];

            if !item.is_nan() {
                if !min_heap.is_empty() && item < min_heap.peek().unwrap().0 {
                    println!("push max: {:?}", item);
                    max_heap.push(item);
                    if max_heap.len() as f32
                        > ((max_heap.len() + min_heap.len()) as f32 * self.quantile) + 0.5
                    {
                        println!("balance min: {:?}", item);
                        min_heap.push(Reverse(max_heap.pop().unwrap()));
                    }
                } else {
                    println!("push min : {:?}", item);
                    min_heap.push(Reverse(item));
                    if min_heap.len() as f32
                        > ((max_heap.len() + min_heap.len()) as f32 * (1.0 - self.quantile)) + 0.5
                    {
                        println!("balance max: {:?}", item);
                        max_heap.push(min_heap.pop().unwrap().0);
                    }
                }
            } else if !self.skip_nan {
                self.result = Some(NoirData::NoirType(item));
                self.found_nan = true;
            }
        }
    }
    fn handle_row(&mut self, item: Vec<NoirType>) {
        if !self.found_nan {
            if self.max_heaps.is_none() {
                self.max_heaps = Some(vec![BinaryHeap::new(); item.len()]);
            }
            if self.min_heaps.is_none() {
                self.min_heaps = Some(vec![BinaryHeap::new(); item.len()]);
            }
            if self.columns_nan.is_none() {
                self.columns_nan = Some(vec![false; item.len()]);
            }

            let max_heaps = self.max_heaps.as_mut().unwrap();
            let min_heaps = self.min_heaps.as_mut().unwrap();
            let columns_nan = self.columns_nan.as_mut().unwrap();

            let mut all_nan = true;
            for (i, v) in item.into_iter().enumerate() {
                if !columns_nan[i] {
                    if !v.is_nan() {
                        all_nan = false;
                        if !min_heaps[i].is_empty() && v < min_heaps[i].peek().unwrap().0 {
                            max_heaps[i].push(v);
                            if max_heaps[i].len() as f32
                                > ((max_heaps[i].len() + min_heaps[i].len()) as f32 * self.quantile) + 0.5
                            {
                                min_heaps[i].push(Reverse(max_heaps[i].pop().unwrap()));
                            }
                        } else {
                            min_heaps[i].push(Reverse(v));
                            if min_heaps[i].len() as f32
                                > ((max_heaps[i].len() + min_heaps[i].len()) as f32
                                    * (1.0 - self.quantile)) + 0.5
                            {
                                max_heaps[i].push(min_heaps[i].pop().unwrap().0);
                            }
                        }
                    } else if !self.skip_nan {
                        columns_nan[i] = true;
                    }
                }
            }
            if all_nan {
                self.result = Some(NoirData::Row(vec![NoirType::NaN(); columns_nan.len()]))
            }
            self.found_nan = all_nan
        }
    }

    fn handle_end(&mut self) {
        if !self.found_nan && self.max_heaps.is_some() {
            let num_col = self.max_heaps.as_ref().unwrap().len();
            let mut result = Vec::with_capacity(num_col);
            let column_nan = self.columns_nan.take().unwrap_or_default();
            let mut max_heap = self.max_heaps.take().unwrap_or_default();
            let mut min_heap = self.min_heaps.take().unwrap_or_default();
            println!("max: {:?}, min: {:?}", max_heap, min_heap);
            for i in 0..num_col {
                if num_col > 1 && column_nan[i] {
                    result.push(NoirType::NaN());
                } else {
                    match (max_heap[i].len() as f32)
                        .partial_cmp(
                            &((max_heap[i].len() + min_heap[i].len()) as f32 * self.quantile),
                        )
                        .unwrap()
                    {
                        std::cmp::Ordering::Less => result.push(min_heap[i].pop().unwrap().0),
                        std::cmp::Ordering::Greater => result.push(max_heap[i].pop().unwrap()),
                        std::cmp::Ordering::Equal => {
                            result.push(
                                (max_heap[i].pop().unwrap() + min_heap[i].pop().unwrap().0) / 2.0,
                            );
                        }
                    }
                }
            }
            if num_col == 1 {
                self.result = Some(NoirData::NoirType(result[0]));
            } else {
                self.result = Some(NoirData::Row(result));
            }
        }
    }
}

impl<PreviousOperators> Operator<NoirData> for MedianExactNoirData<PreviousOperators>
where
    PreviousOperators: Operator<NoirData>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<NoirData> {
        while !self.received_end {
            match self.prev.next() {
                StreamElement::Item(item) => match item {
                    NoirData::Row(row) => self.handle_row(row),
                    NoirData::NoirType(it) => self.handle_noir_type(it),
                },
                StreamElement::Timestamped(item, ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    match item {
                        NoirData::Row(row) => self.handle_row(row),
                        NoirData::NoirType(it) => self.handle_noir_type(it),
                    }
                }
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                StreamElement::Terminate => {
                    self.received_end = true;
                    self.handle_end();
                }
                StreamElement::FlushBatch => {}
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    self.handle_end();
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
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<NoirData, _>(
                "Median_Exact_Noir_Data",
            ))
    }
}

impl<PreviousOperators> Clone for MedianExactNoirData<PreviousOperators>
where
    PreviousOperators: Operator<NoirData>,
{
    fn clone(&self) -> Self {
        panic!("MedianExact_Noir_Data cannot be cloned, max_parallelism should be 1");
    }
}

impl<Op> Stream<NoirData, Op>
where
    Op: Operator<NoirData> + 'static,
{
    pub fn quantile_parallel(
        self,
        quantile: f32,
        skip_nan: bool,
    ) -> Stream<NoirData, impl Operator<NoirData>> {
        self.flat_map(|item| match item {
            NoirData::Row(row) => {
                let mut res = Vec::with_capacity(row.len());
                for (i, el) in row.iter().enumerate() {
                    res.push((i, NoirData::NoirType(*el)));
                }
                res
            }
            NoirData::NoirType(_) => vec![(0, item)],
        })
        .group_by(|it| it.0)
        .fold(
            (
                false,
                BinaryHeap::new(),
                BinaryHeap::<Reverse<NoirType>>::new(),
            ),
            move |acc, item| {
                if !acc.0 {
                    let max_heap = &mut acc.1;
                    let min_heap = &mut acc.2;
                    let item = item.1.to_type();

                    if !item.is_na() {
                        if !min_heap.is_empty() && item < min_heap.peek().unwrap().0 {
                            max_heap.push(item);
                            if max_heap.len() as f32
                                > ((max_heap.len() + min_heap.len()) as f32 * quantile) + 0.5
                            {
                                min_heap.push(Reverse(max_heap.pop().unwrap()));
                            }
                        } else {
                            min_heap.push(Reverse(item));
                            if min_heap.len() as f32
                                > ((max_heap.len() + min_heap.len()) as f32 * (1.0 - quantile)) + 0.5
                            {
                                max_heap.push(min_heap.pop().unwrap().0);
                            }
                        }
                    } else if !skip_nan {
                        acc.0 = true;
                    }
                }
            },
        )
        .map(move |(_, item)| {
            if !item.0 {
                let mut max_heap = item.1;
                let mut min_heap = item.2;
                if max_heap.is_empty() && min_heap.is_empty() {
                    NoirData::NoirType(NoirType::None())
                } else {
                    match (max_heap.len() as f32)
                        .partial_cmp(&((max_heap.len() + min_heap.len()) as f32 * quantile))
                        .unwrap()
                    {
                        std::cmp::Ordering::Less => NoirData::NoirType(min_heap.pop().unwrap().0),
                        std::cmp::Ordering::Greater => NoirData::NoirType(max_heap.pop().unwrap()),
                        std::cmp::Ordering::Equal => NoirData::NoirType(
                            (max_heap.pop().unwrap() + min_heap.pop().unwrap().0) / 2.0,
                        ),
                    }
                }
            } else {
                NoirData::NoirType(NoirType::NaN())
            }
        })
        .unkey()
        .fold(Vec::new(), |acc, item| {
            if acc.len() <= item.0 {
                acc.resize(item.0 + 1, NoirType::None())
            }
            let v = acc.get_mut(item.0).unwrap();
            *v = item.1.to_type();
        })
        .map(|item| {
            if item.len() == 1 {
                NoirData::NoirType(item[0])
            } else {
                NoirData::Row(item)
            }
        })
    }

    /// Reduce the stream of NoirData to its median value.
    ///
    /// skip_nan: if true, NaN values will not be considered, otherwise they will be considered as the median value.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: since this operator compute the exact median it cannot be parallelized.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::data_type::{NoirData, NoirType};
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new([NoirData::NoirType(NoirType::from(0.0)), NoirData::NoirType(NoirType::from(8.0)), NoirData::NoirType(NoirType::from(6.0))].into_iter()));
    /// let res = s.quantile_exact(0.5, true).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![NoirData::NoirType(NoirType::from(6.0))]);
    /// ```
    pub fn quantile_exact(
        self,
        quantile: f32,
        skip_nan: bool,
    ) -> Stream<NoirData, MedianExactNoirData<SimpleStartOperator<NoirData>>> {
        self.replication(Replication::One)
            .add_operator(|prev| MedianExactNoirData::new(prev, quantile, skip_nan))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        data_type::NoirType,
        operator::Operator,
        operator::{median_exact::MedianExact, StreamElement},
        test::FakeOperator,
    };

    #[test]
    fn median_exact_float() {
        let fake_operator = FakeOperator::new(
            [
                NoirType::Float32(1.0),
                NoirType::Float32(2.0),
                NoirType::Float32(3.0),
                NoirType::Float32(4.0),
                NoirType::Float32(1.0),
                NoirType::Float32(2.0),
            ]
            .into_iter(),
        );
        let mut median = MedianExact::new(fake_operator, |v| v);

        assert_eq!(median.next(), StreamElement::Item(NoirType::Float32(2.0)));
        assert_eq!(median.next(), StreamElement::Terminate);
    }

    #[test]
    fn median_exact_int() {
        let fake_operator = FakeOperator::new(
            [NoirType::Int32(1), NoirType::Int32(2), NoirType::Int32(4)].into_iter(),
        );
        let mut median = MedianExact::new(fake_operator, |v| v);

        assert_eq!(median.next(), StreamElement::Item(NoirType::Int32(2)));
        assert_eq!(median.next(), StreamElement::Terminate);
    }

    #[test]
    fn median_exact_single() {
        let fake_operator = FakeOperator::new([NoirType::Int32(1)].into_iter());
        let mut median = MedianExact::new(fake_operator, |v| v);

        assert_eq!(median.next(), StreamElement::Item(NoirType::Int32(1)));
        assert_eq!(median.next(), StreamElement::Terminate);
    }

    use crate::data_type::NoirData;
    use crate::operator::median_exact::MedianExactNoirData;

    #[test]
    fn test_median_row() {
        let rows = vec![
            NoirData::new(
                [
                    NoirType::from(0.0),
                    NoirType::from(8.0),
                    NoirType::from(6.0),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                    NoirType::from(6.0),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(2.0),
                    NoirType::from(f32::NAN),
                    NoirType::from(6.0),
                    NoirType::from(9.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(3.0),
                    NoirType::from(2.0),
                    NoirType::from(6.0),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
        ];
        let fake_operator = FakeOperator::new(rows.iter().cloned());
        let mut median = MedianExactNoirData::new(fake_operator, 0.5, true);

        assert_eq!(
            median.next(),
            StreamElement::Item(NoirData::new(
                [
                    NoirType::from(1.5),
                    NoirType::from(4.0),
                    NoirType::from(6.0),
                    NoirType::from(4.0)
                ]
                .to_vec()
            ))
        );
        assert_eq!(median.next(), StreamElement::Terminate);
    }

    #[test]
    fn test_median_row_nan() {
        let rows = vec![
            NoirData::new(
                [
                    NoirType::from(0.0),
                    NoirType::from(8.0),
                    NoirType::from(f32::NAN),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                    NoirType::from(6.0),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(f32::NAN),
                    NoirType::from(1.0),
                    NoirType::from(6.0),
                    NoirType::from(9.0),
                ]
                .to_vec(),
            ),
            NoirData::new(
                [
                    NoirType::from(3.0),
                    NoirType::from(2.0),
                    NoirType::from(6.0),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
        ];
        let fake_operator = FakeOperator::new(rows.iter().cloned());
        let mut median = MedianExactNoirData::new(fake_operator, 0.5, false);

        assert_eq!(
            median.next(),
            StreamElement::Item(NoirData::new(
                [
                    NoirType::from(f32::NAN),
                    NoirType::from(3.0),
                    NoirType::from(f32::NAN),
                    NoirType::from(4.0)
                ]
                .to_vec()
            ))
        );
        assert_eq!(median.next(), StreamElement::Terminate);
    }

    #[test]
    fn test_median_skip_nan() {
        let fake_operator = FakeOperator::new(
            [
                NoirData::NoirType(NoirType::from(0.0)),
                NoirData::NoirType(NoirType::from(8.0)),
                NoirData::NoirType(NoirType::from(f32::NAN)),
                NoirData::NoirType(NoirType::from(6.0)),
                NoirData::NoirType(NoirType::from(4.0)),
            ]
            .iter()
            .cloned(),
        );
        let mut median = MedianExactNoirData::new(fake_operator, 0.5, true);

        assert_eq!(
            median.next(),
            StreamElement::Item(NoirData::NoirType(NoirType::from(5.0)))
        );
        assert_eq!(median.next(), StreamElement::Terminate);
    }

    #[test]
    fn test_median_nan() {
        let fake_operator = FakeOperator::new(
            [
                NoirData::NoirType(NoirType::from(0.0)),
                NoirData::NoirType(NoirType::from(8.0)),
                NoirData::NoirType(NoirType::from(f32::NAN)),
                NoirData::NoirType(NoirType::from(6.0)),
                NoirData::NoirType(NoirType::from(4.0)),
            ]
            .iter()
            .cloned(),
        );
        let mut median = MedianExactNoirData::new(fake_operator, 0.5, false);

        assert_eq!(
            median.next(),
            StreamElement::Item(NoirData::NoirType(NoirType::from(f32::NAN)))
        );
        assert_eq!(median.next(), StreamElement::Terminate);
    }

    #[test]
    fn test_median_without_timestamps() {
        let fake_operator = FakeOperator::new(
            [
                NoirData::NoirType(NoirType::from(8.0)),
                NoirData::NoirType(NoirType::from(0.0)),
                NoirData::NoirType(NoirType::from(2.0)),
                NoirData::NoirType(NoirType::from(6.0)),
                NoirData::NoirType(NoirType::from(4.0)),
            ]
            .iter()
            .cloned(),
        );
        let mut median = MedianExactNoirData::new(fake_operator, 0.5, true);

        assert_eq!(
            median.next(),
            StreamElement::Item(NoirData::NoirType(NoirType::from(4.0)))
        );
        assert_eq!(median.next(), StreamElement::Terminate);
    }

    #[test]
    #[allow(clippy::identity_op)]
    #[cfg(feature = "timestamp")]
    fn test_median_timestamped() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped(
            NoirData::NoirType(NoirType::from(0)),
            1,
        ));
        fake_operator.push(StreamElement::Timestamped(
            NoirData::NoirType(NoirType::from(1)),
            2,
        ));
        fake_operator.push(StreamElement::Timestamped(
            NoirData::NoirType(NoirType::from(2)),
            3,
        ));
        fake_operator.push(StreamElement::Watermark(4));

        let mut median = MedianExactNoirData::new(fake_operator, 0.5, true);

        assert_eq!(
            median.next(),
            StreamElement::Timestamped(NoirData::NoirType(NoirType::from(1)), 3)
        );
        assert_eq!(median.next(), StreamElement::Watermark(4));
        assert_eq!(median.next(), StreamElement::Terminate);
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_median_iter_end() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item(NoirData::NoirType(NoirType::from(0))));
        fake_operator.push(StreamElement::Item(NoirData::NoirType(NoirType::from(1))));
        fake_operator.push(StreamElement::Item(NoirData::NoirType(NoirType::from(2))));
        fake_operator.push(StreamElement::FlushAndRestart);
        fake_operator.push(StreamElement::Item(NoirData::NoirType(NoirType::from(3))));
        fake_operator.push(StreamElement::Item(NoirData::NoirType(NoirType::from(4))));
        fake_operator.push(StreamElement::Item(NoirData::NoirType(NoirType::from(5))));
        fake_operator.push(StreamElement::FlushAndRestart);

        let mut median = MedianExactNoirData::new(fake_operator, 0.5, true);

        assert_eq!(
            median.next(),
            StreamElement::Item(NoirData::NoirType(NoirType::from(1)))
        );
        assert_eq!(median.next(), StreamElement::FlushAndRestart);
        assert_eq!(
            median.next(),
            StreamElement::Item(NoirData::NoirType(NoirType::from(4)))
        );
        assert_eq!(median.next(), StreamElement::FlushAndRestart);
        assert_eq!(median.next(), StreamElement::Terminate);
    }
}
