use std::fmt::Display;

use crate::{
    block::{BlockStructure, OperatorStructure},
    data_type::NoirData,
    ExecutionMetadata, Stream,
};

use super::{fold::Fold, Operator, StreamElement};
use crate::operator::Timestamp;

impl<Op> Stream<NoirData, Op>
where
    Op: Operator<NoirData> + 'static,
{
    pub fn std_dev(self, skip_na: bool) -> Stream<NoirData, impl Operator<NoirData>> {
        self.variance(skip_na).map(|v| match v {
            NoirData::NoirType(v) => NoirData::NoirType(v.sqrt()),
            NoirData::Row(v) => {
                let mut result = Vec::with_capacity(v.len());
                for i in v {
                    result.push(i.sqrt());
                }
                NoirData::Row(result)
            }
        })
    }

    pub fn variance(self, skip_na: bool) -> Stream<NoirData, impl Operator<NoirData>> {
        self.add_operator(|prev| Welford::new(prev, skip_na))
            .replication(crate::Replication::One)
            .add_operator(|prev| {
                Fold::new(
                    prev,
                    (None::<NoirData>, None::<NoirData>, None::<NoirData>, false),
                    move |acc, item| {
                        let (count, mean, m2, found_nan) = acc;
                        if !*found_nan {
                            NoirData::chen(count, mean, m2, skip_na, item);
                        }
                    },
                )
            })
            .map(|value| {
                let (count, mean, m2, _) = value;
                match (count, mean, m2) {
                    (
                        Some(NoirData::NoirType(count)),
                        Some(NoirData::NoirType(mean)),
                        Some(NoirData::NoirType(m2)),
                    ) => {
                        if count.is_na() || m2.is_na() || mean.is_na() {
                            return NoirData::NoirType(mean);
                        }
                        NoirData::NoirType(m2 / (count - 1))
                    }
                    (
                        Some(NoirData::Row(count)),
                        Some(NoirData::Row(mean)),
                        Some(NoirData::Row(m2)),
                    ) => {
                        let mut result = Vec::with_capacity(count.len());
                        for (i, v) in count.into_iter().enumerate() {
                            if v.is_na() || m2[i].is_na() || mean[i].is_na() {
                                result.push(mean[i]);
                            } else {
                                result.push(m2[i] / (v - 1));
                            }
                        }
                        NoirData::Row(result)
                    }
                    _ => panic!("Fatal error in Entropy"),
                }
            })
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Welford<PreviousOperators>
where
    PreviousOperators: Operator<NoirData>,
{
    prev: PreviousOperators,
    mean: Option<NoirData>,
    count: Option<NoirData>,
    m2: Option<NoirData>,
    found_nan: bool,
    skip_na: bool,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
}

impl<PreviousOperators> Display for Welford<PreviousOperators>
where
    PreviousOperators: Operator<NoirData>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Welford<{}>",
            self.prev,
            std::any::type_name::<NoirData>(),
        )
    }
}

impl<PreviousOperators: Operator<NoirData>> Welford<PreviousOperators> {
    pub fn new(prev: PreviousOperators, skip_na: bool) -> Self {
        Self {
            prev,
            mean: None,
            count: None,
            m2: None,
            found_nan: false,
            skip_na,
            timestamp: None,
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
        }
    }
}

impl<PreviousOperators> Operator<(NoirData, NoirData, NoirData)> for Welford<PreviousOperators>
where
    PreviousOperators: Operator<NoirData>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<(NoirData, NoirData, NoirData)> {
        while !self.received_end {
            match self.prev.next() {
                StreamElement::Terminate => self.received_end = true,
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    self.received_end_iter = true;
                }
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts));
                }
                StreamElement::Item(item) => {
                    if !self.found_nan {
                        self.found_nan = item.welford(
                            &mut self.count,
                            &mut self.mean,
                            &mut self.m2,
                            self.skip_na,
                        );
                    }
                }
                StreamElement::Timestamped(item, ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    if !self.found_nan {
                        self.found_nan = item.welford(
                            &mut self.count,
                            &mut self.mean,
                            &mut self.m2,
                            self.skip_na,
                        );
                    }
                }
                // this block wont sent anything until the stream ends
                StreamElement::FlushBatch => {}
            }
        }

        // If there is an accumulated value, return it
        if let Some(acc) = self.mean.take() {
            let count = self.count.take().unwrap();
            let m2 = self.m2.take().unwrap();
            if let Some(ts) = self.timestamp.take() {
                return StreamElement::Timestamped((count, acc, m2), ts);
            } else {
                return StreamElement::Item((count, acc, m2));
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
            .add_operator(OperatorStructure::new::<NoirData, _>("Welford"))
    }
}
