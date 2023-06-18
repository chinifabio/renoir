use std::fmt::Display;

use super::{fold::Fold, Data, ExchangeData, Operator, StreamElement, Timestamp};
use crate::{
    block::{BlockStructure, OperatorStructure},
    data_type::{NoirType, Row},
    ExecutionMetadata, Stream,
};

impl<I, Op> Stream<I, Op>
where
    I: Data,
    Op: Operator<I> + 'static,
{
    /// Reduce the stream into a stream that emits a single value which is the maximum value of the stream.
    ///
    /// The reducing operator consists in scanning the stream and keeping track of the maximum value.
    ///
    /// The "get_value" function is used to access the values that will be compared to the current maximum.
    /// The function should return an implementation of the Ord trait.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator is not parallelized, it creates a bottleneck where all the stream
    /// elements are sent to and the folding is done using a single thread.
    ///
    /// **Note**: this is very similar to [`Iteartor::max`](std::iter::Iterator::max).
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.max(|&n| n).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![4]);
    /// ```
    pub fn max<F, D>(self, get_value: F) -> Stream<I, impl Operator<I>>
    where
        F: Fn(&I) -> D + Clone + Send + 'static,
        I: ExchangeData,
        D: Ord,
    {
        self.max_parallelism(1)
            .add_operator(|prev| {
                Fold::new(prev, None, move |acc, b| {
                    *acc = Some(if let Some(a) = acc.take() {
                        if get_value(&b) > get_value(&a) {
                            b
                        } else {
                            a
                        }
                    } else {
                        b
                    })
                })
            })
            .map(|value| value.unwrap())
    }

    /// Reduce the stream into a stream that emits a single value which is the maximum value of the stream.
    ///
    /// The reducing operator consists in scanning the stream and keeping track of the maximum value.
    ///
    /// The "get_value" function is used to access the values that will be compared to the current maximum.
    /// The function should return an implementation of the Ord trait.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this is very similar to [`Iteartor::max`](std::iter::Iterator::max).
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.max_assoc(|&n| n).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![4]);
    /// ```
    pub fn max_assoc<F, D>(self, get_value: F) -> Stream<I, impl Operator<I>>
    where
        F: Fn(&I) -> D + Clone + Send + Copy + 'static,
        I: ExchangeData,
        D: Ord,
    {
        self.add_operator(|prev| {
            Fold::new(prev, None, move |acc, b| {
                *acc = Some(if let Some(a) = acc.take() {
                    if get_value(&b) > get_value(&a) {
                        b
                    } else {
                        a
                    }
                } else {
                    b
                })
            })
        })
        .map(|value| value.unwrap())
        .max_parallelism(1)
        .add_operator(|prev| {
            Fold::new(prev, None, move |acc, b| {
                *acc = Some(if let Some(a) = acc.take() {
                    if get_value(&b) > get_value(&a) {
                        b
                    } else {
                        a
                    }
                } else {
                    b
                })
            })
        })
        .map(|value| value.unwrap())
    }
}

impl<Op> Stream<NoirType, Op>
where
    Op: Operator<NoirType> + 'static,
{
    /// Reduce the stream into a stream that emits a single value which is the maximum value of the stream.
    ///
    /// The reducing operator consists in scanning the stream and keeping track of the maximum value.
    ///
    /// skip_nan: if true, NaN values will be ignored and not considered as the maximum value.
    /// else, the operator will output NaN if as soon as a NaN value is found.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator is not parallelized, it creates a bottleneck where all the stream
    /// elements are sent to and the folding is done using a single thread.
    ///
    /// **Note**: this is very similar to [`Iteartor::max`](std::iter::Iterator::max).
    ///
    /// **Note**: this operator will split the current block.
    pub fn max_noir(self, skip_nan: bool) -> Stream<NoirType, impl Operator<NoirType>> {
        self.max_parallelism(1)
            .add_operator(|prev| Max::new(prev, skip_nan))
    }

    /// Reduce the stream into a stream that emits a single value which is the maximum value of the stream.
    ///
    /// The reducing operator consists in scanning the stream and keeping track of the maximum value.
    ///
    /// skip_nan: if true, NaN values will be ignored and not considered as the maximum value.
    /// else, the operator will output NaN if as soon as a NaN value is found.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this is very similar to [`Iteartor::max`](std::iter::Iterator::max).
    ///
    /// **Note**: this operator will split the current block.
    pub fn max_noir_assoc(self, skip_nan: bool) -> Stream<NoirType, impl Operator<NoirType>> {
        self.add_operator(|prev| Max::new(prev, skip_nan))
            .max_parallelism(1)
            .add_operator(|prev| Max::new(prev, skip_nan))
    }
}

///Max operator for a stream of ['Row'] (crate::dataflow::Row).
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct MaxRow<PreviousOperators>
where
    PreviousOperators: Operator<Row>,
{
    prev: PreviousOperators,
    accumulator: Option<Row>,
    skip_nan: bool,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
}

impl<PreviousOperators> Display for MaxRow<PreviousOperators>
where
    PreviousOperators: Operator<Row>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> Max<{}>", self.prev, std::any::type_name::<Row>(),)
    }
}

impl<PreviousOperators: Operator<Row>> MaxRow<PreviousOperators> {
    pub fn new(prev: PreviousOperators, skip_nan: bool) -> Self {
        Self {
            prev,
            accumulator: None,
            skip_nan,
            timestamp: None,
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
        }
    }
}

impl<PreviousOperators> Operator<Row> for MaxRow<PreviousOperators>
where
    PreviousOperators: Operator<Row>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Row> {
        while !self.received_end {
            match self.prev.next() {
                StreamElement::Terminate => self.received_end = true,
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    self.received_end_iter = true;
                }
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                StreamElement::Item(item) => {
                    if self.accumulator.is_none() {
                        self.accumulator = Some(item);
                    } else {
                        for (i, v) in item.columns.into_iter().enumerate() {
                            if !&self.accumulator.as_ref().unwrap().columns[i].is_nan()
                                && ((!v.is_nan()
                                    && v > self.accumulator.as_ref().unwrap().columns[i])
                                    || (v.is_nan() && !self.skip_nan))
                            {
                                self.accumulator.as_mut().unwrap().columns[i] = v;
                            }
                        }
                    }
                }
                StreamElement::Timestamped(item, ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    if self.accumulator.is_none() {
                        self.accumulator = Some(item);
                    } else {
                        for (i, v) in item.columns.into_iter().enumerate() {
                            if !&self.accumulator.as_ref().unwrap().columns[i].is_nan()
                                && ((!v.is_nan()
                                    && v > self.accumulator.as_ref().unwrap().columns[i])
                                    || (v.is_nan() && !self.skip_nan))
                            {
                                self.accumulator.as_mut().unwrap().columns[i] = v;
                            }
                        }
                    }
                }
                // this block wont sent anything until the stream ends
                StreamElement::FlushBatch => {}
            }
        }

        // If there is an accumulated value, return it
        if let Some(acc) = self.accumulator.take() {
            if let Some(ts) = self.timestamp.take() {
                return StreamElement::Timestamped(acc, ts);
            } else {
                return StreamElement::Item(acc);
            }
        }

        // If watermark were received, send one downstream
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
            .add_operator(OperatorStructure::new::<Row, _>("MaxRow"))
    }
}

/// Max operator for a stream of [`NoirType`](crate::dataflow::NoirType).
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Max<PreviousOperators>
where
    PreviousOperators: Operator<NoirType>,
{
    prev: PreviousOperators,
    accumulator: Option<NoirType>,
    skip_nan: bool,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
}

impl<PreviousOperators> Display for Max<PreviousOperators>
where
    PreviousOperators: Operator<NoirType>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Max<{}>",
            self.prev,
            std::any::type_name::<NoirType>(),
        )
    }
}

impl<PreviousOperators: Operator<NoirType>> Max<PreviousOperators> {
    pub(super) fn new(prev: PreviousOperators, skip_nan: bool) -> Self {
        Max {
            prev,
            accumulator: None,
            skip_nan,
            timestamp: None,
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
        }
    }
}

impl<PreviousOperators> Operator<NoirType> for Max<PreviousOperators>
where
    PreviousOperators: Operator<NoirType>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<NoirType> {
        while !self.received_end {
            match self.prev.next() {
                StreamElement::Terminate => self.received_end = true,
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    self.received_end_iter = true;
                }
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                StreamElement::Item(item) => {
                    if self.accumulator.is_none() {
                        if !item.is_nan() {
                            self.accumulator = Some(item);
                        } else if !self.skip_nan {
                            self.accumulator = Some(item);
                            self.received_end = true;
                        }
                    } else if !item.is_nan() && item > self.accumulator.clone().unwrap() {
                        self.accumulator = Some(item);
                    } else if item.is_nan() && !self.skip_nan {
                        self.accumulator = Some(item);
                        self.received_end = true;
                    }
                }
                StreamElement::Timestamped(item, ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    if self.accumulator.is_none() {
                        if !item.is_nan() {
                            self.accumulator = Some(item);
                        } else if !self.skip_nan {
                            self.accumulator = Some(item);
                            self.received_end = true;
                        }
                    } else if !item.is_nan() && item > self.accumulator.clone().unwrap() {
                        self.accumulator = Some(item);
                    } else if item.is_nan() && !self.skip_nan {
                        self.accumulator = Some(item);
                        self.received_end = true;
                    }
                }
                // this block wont sent anything until the stream ends
                StreamElement::FlushBatch => {}
            }
        }

        // If there is an accumulated value, return it
        if let Some(acc) = self.accumulator.take() {
            if let Some(ts) = self.timestamp.take() {
                return StreamElement::Timestamped(acc, ts);
            } else {
                return StreamElement::Item(acc);
            }
        }

        // If watermark were received, send one downstream
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
            .add_operator(OperatorStructure::new::<NoirType, _>("Max"))
    }
}

#[cfg(test)]
mod tests {
    use crate::data_type::{NoirType, Row};
    use crate::operator::max::{Max, MaxRow};
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_max_row() {
        let rows = vec![
            Row::new(
                [
                    NoirType::from(0.0),
                    NoirType::from(8.0),
                    NoirType::from(6.0),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            Row::new(
                [
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                    NoirType::from(6.0),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            Row::new(
                [
                    NoirType::from(2.0),
                    NoirType::from(f32::NAN),
                    NoirType::from(6.0),
                    NoirType::from(9.0),
                ]
                .to_vec(),
            ),
            Row::new(
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
        let mut max = MaxRow::new(fake_operator, true);

        assert_eq!(
            max.next(),
            StreamElement::Item(Row::new(
                [
                    NoirType::from(3.0),
                    NoirType::from(8.0),
                    NoirType::from(6.0),
                    NoirType::from(9.0)
                ]
                .to_vec()
            ))
        );
        assert_eq!(max.next(), StreamElement::Terminate);
    }

    #[test]
    fn test_max_row_nan() {
        let rows = vec![
            Row::new(
                [
                    NoirType::from(0.0),
                    NoirType::from(8.0),
                    NoirType::from(f32::NAN),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            Row::new(
                [
                    NoirType::from(1.0),
                    NoirType::from(4.0),
                    NoirType::from(6.0),
                    NoirType::from(4.0),
                ]
                .to_vec(),
            ),
            Row::new(
                [
                    NoirType::from(f32::NAN),
                    NoirType::from(1.0),
                    NoirType::from(6.0),
                    NoirType::from(9.0),
                ]
                .to_vec(),
            ),
            Row::new(
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
        let mut max = MaxRow::new(fake_operator, false);

        assert_eq!(
            max.next(),
            StreamElement::Item(Row::new(
                [
                    NoirType::from(f32::NAN),
                    NoirType::from(8.0),
                    NoirType::from(f32::NAN),
                    NoirType::from(9.0)
                ]
                .to_vec()
            ))
        );
        assert_eq!(max.next(), StreamElement::Terminate);
    }

    #[test]
    fn test_max_skip_nan() {
        let fake_operator = FakeOperator::new(
            [
                NoirType::from(0.0),
                NoirType::from(8.0),
                NoirType::from(f32::NAN),
                NoirType::from(6.0),
                NoirType::from(4.0),
            ]
            .iter()
            .cloned(),
        );
        let mut max = Max::new(fake_operator, true);

        assert_eq!(max.next(), StreamElement::Item(NoirType::from(8.0)));
        assert_eq!(max.next(), StreamElement::Terminate);
    }

    #[test]
    fn test_max_nan() {
        let fake_operator = FakeOperator::new(
            [
                NoirType::from(0.0),
                NoirType::from(8.0),
                NoirType::from(f32::NAN),
                NoirType::from(6.0),
                NoirType::from(4.0),
            ]
            .iter()
            .cloned(),
        );
        let mut max = Max::new(fake_operator, false);

        assert_eq!(max.next(), StreamElement::Item(NoirType::from(f32::NAN)));
        assert_eq!(max.next(), StreamElement::Terminate);
    }

    #[test]
    fn test_max_without_timestamps() {
        let fake_operator = FakeOperator::new(
            [
                NoirType::from(0.0),
                NoirType::from(8.0),
                NoirType::from(2.0),
                NoirType::from(6.0),
                NoirType::from(4.0),
            ]
            .iter()
            .cloned(),
        );
        let mut max = Max::new(fake_operator, true);

        assert_eq!(max.next(), StreamElement::Item(NoirType::from(8.0)));
        assert_eq!(max.next(), StreamElement::Terminate);
    }

    #[test]
    #[allow(clippy::identity_op)]
    #[cfg(feature = "timestamp")]
    fn test_max_timestamped() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped(NoirType::from(0), 1));
        fake_operator.push(StreamElement::Timestamped(NoirType::from(1), 2));
        fake_operator.push(StreamElement::Timestamped(NoirType::from(2), 3));
        fake_operator.push(StreamElement::Watermark(4));

        let mut max = Max::new(fake_operator, true);

        assert_eq!(max.next(), StreamElement::Timestamped(NoirType::from(2), 3));
        assert_eq!(max.next(), StreamElement::Watermark(4));
        assert_eq!(max.next(), StreamElement::Terminate);
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_max_iter_end() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item(NoirType::from(0)));
        fake_operator.push(StreamElement::Item(NoirType::from(1)));
        fake_operator.push(StreamElement::Item(NoirType::from(2)));
        fake_operator.push(StreamElement::FlushAndRestart);
        fake_operator.push(StreamElement::Item(NoirType::from(3)));
        fake_operator.push(StreamElement::Item(NoirType::from(4)));
        fake_operator.push(StreamElement::Item(NoirType::from(5)));
        fake_operator.push(StreamElement::FlushAndRestart);

        let mut max = Max::new(fake_operator, true);

        assert_eq!(max.next(), StreamElement::Item(NoirType::from(2)));
        assert_eq!(max.next(), StreamElement::FlushAndRestart);
        assert_eq!(max.next(), StreamElement::Item(NoirType::from(5)));
        assert_eq!(max.next(), StreamElement::FlushAndRestart);
        assert_eq!(max.next(), StreamElement::Terminate);
    }
}
