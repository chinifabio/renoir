use std::fmt::Display;

use super::{
    fold::Fold, Data, ExchangeData, Operator, SimpleStartOperator, StreamElement, Timestamp,
};
use crate::data_type::noir_data::NoirData;
use crate::{
    block::{BlockStructure, OperatorStructure},
    ExecutionMetadata, Replication, Stream,
};

impl<I, Op> Stream<Op>
where
    I: Data,
    Op: Operator<Out = I> + 'static,
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
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![4]);
    /// ```
    pub fn max<F, D>(self, get_value: F) -> Stream<impl Operator<Out = I>>
    where
        F: Fn(&I) -> D + Clone + Send + 'static,
        I: ExchangeData,
        D: Ord,
    {
        self.replication(Replication::One)
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
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![4]);
    /// ```
    pub fn max_assoc<F, D>(self, get_value: F) -> Stream<impl Operator<Out = I>>
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
        .replication(Replication::One)
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

impl<Op> Stream<Op>
where
    Op: Operator<Out = NoirData> + 'static,
{
    /// Reduce the stream of NoirData into a stream that emits a single value which is the maximum value of the stream.
    ///
    /// The reducing operator consists in scanning the stream and keeping track of the maximum value.
    ///
    /// skip_na: if true, NaN values will not be considered, otherwise they will be considered as the maximum value.
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
    /// # use noir::data_type::{NoirData, NoirType};
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new([NoirData::NoirType(NoirType::from(0.0)), NoirData::NoirType(NoirType::from(8.0)), NoirData::NoirType(NoirType::from(6.0))].into_iter()));
    /// let res = s.max_noir_data(true).collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![NoirData::NoirType(NoirType::from(8.0))]);
    /// ```
    pub fn max_noir_data(
        self,
        skip_na: bool,
    ) -> Stream<MaxNoirData<SimpleStartOperator<NoirData>>> {
        self.add_operator(|prev| MaxNoirData::new(prev, skip_na))
            .replication(Replication::One)
            .add_operator(|prev| MaxNoirData::new(prev, skip_na))
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct MaxNoirData<PreviousOperators>
where
    PreviousOperators: Operator<Out = NoirData>,
{
    prev: PreviousOperators,
    max_item: Option<NoirData>,
    found_nan: bool,
    skip_na: bool,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
}

impl<PreviousOperators> Display for MaxNoirData<PreviousOperators>
where
    PreviousOperators: Operator<Out = NoirData>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Max<{}>",
            self.prev,
            std::any::type_name::<NoirData>(),
        )
    }
}

impl<PreviousOperators: Operator<Out = NoirData>> MaxNoirData<PreviousOperators> {
    pub fn new(prev: PreviousOperators, skip_na: bool) -> Self {
        Self {
            prev,
            max_item: None,
            found_nan: false,
            skip_na,
            timestamp: None,
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
        }
    }
}

impl<PreviousOperators> Operator for MaxNoirData<PreviousOperators>
where
    PreviousOperators: Operator<Out = NoirData>,
{
    type Out = NoirData;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<NoirData> {
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
                    if !self.found_nan {
                        self.found_nan = item.max(&mut self.max_item, self.skip_na)
                    }
                }
                StreamElement::Timestamped(item, ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    if !self.found_nan {
                        self.found_nan = item.max(&mut self.max_item, self.skip_na)
                    }
                }
                // this block wont sent anything until the stream ends
                StreamElement::FlushBatch => {}
            }
        }

        // If there is an accumulated value, return it
        if let Some(acc) = self.max_item.take() {
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
            .add_operator(OperatorStructure::new::<NoirData, _>("Max_NoirDataType"))
    }
}

#[cfg(test)]
mod tests {
    use crate::data_type::noir_data::NoirData;
    use crate::data_type::noir_type::NoirType;
    use crate::operator::max::MaxNoirData;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_max_row() {
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
        let mut max = MaxNoirData::new(fake_operator, true);

        assert_eq!(
            max.next(),
            StreamElement::Item(NoirData::new(
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
        let mut max = MaxNoirData::new(fake_operator, false);

        assert_eq!(
            max.next(),
            StreamElement::Item(NoirData::new(
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
    fn test_max_skip_na() {
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
        let mut max = MaxNoirData::new(fake_operator, true);

        assert_eq!(
            max.next(),
            StreamElement::Item(NoirData::NoirType(NoirType::from(8.0)))
        );
        assert_eq!(max.next(), StreamElement::Terminate);
    }

    #[test]
    fn test_max_nan() {
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
        let mut max = MaxNoirData::new(fake_operator, false);

        assert_eq!(
            max.next(),
            StreamElement::Item(NoirData::NoirType(NoirType::from(f32::NAN)))
        );
        assert_eq!(max.next(), StreamElement::Terminate);
    }

    #[test]
    fn test_max_without_timestamps() {
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
        let mut max = MaxNoirData::new(fake_operator, true);

        assert_eq!(
            max.next(),
            StreamElement::Item(NoirData::NoirType(NoirType::from(8.0)))
        );
        assert_eq!(max.next(), StreamElement::Terminate);
    }

    #[test]
    #[allow(clippy::identity_op)]
    #[cfg(feature = "timestamp")]
    fn test_max_timestamped() {
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

        let mut max = MaxNoirData::new(fake_operator, true);

        assert_eq!(
            max.next(),
            StreamElement::Timestamped(NoirData::NoirType(NoirType::from(2)), 3)
        );
        assert_eq!(max.next(), StreamElement::Watermark(4));
        assert_eq!(max.next(), StreamElement::Terminate);
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_max_iter_end() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item(NoirData::NoirType(NoirType::from(0))));
        fake_operator.push(StreamElement::Item(NoirData::NoirType(NoirType::from(1))));
        fake_operator.push(StreamElement::Item(NoirData::NoirType(NoirType::from(2))));
        fake_operator.push(StreamElement::FlushAndRestart);
        fake_operator.push(StreamElement::Item(NoirData::NoirType(NoirType::from(3))));
        fake_operator.push(StreamElement::Item(NoirData::NoirType(NoirType::from(4))));
        fake_operator.push(StreamElement::Item(NoirData::NoirType(NoirType::from(5))));
        fake_operator.push(StreamElement::FlushAndRestart);

        let mut max = MaxNoirData::new(fake_operator, true);

        assert_eq!(
            max.next(),
            StreamElement::Item(NoirData::NoirType(NoirType::from(2)))
        );
        assert_eq!(max.next(), StreamElement::FlushAndRestart);
        assert_eq!(
            max.next(),
            StreamElement::Item(NoirData::NoirType(NoirType::from(5)))
        );
        assert_eq!(max.next(), StreamElement::FlushAndRestart);
        assert_eq!(max.next(), StreamElement::Terminate);
    }
}
