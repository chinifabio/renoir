use std::{collections::VecDeque, fmt::Display};

use crate::data_type::noir_data::NoirData;
use crate::data_type::noir_type::NoirType;
use crate::operator::Timestamp;
use crate::{
    block::{BlockStructure, OperatorStructure},
    ExecutionMetadata, Stream,
};

use super::{Operator, StreamElement};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
enum FillState {
    #[default]
    None,
    Accumulating(Option<NoirData>),
    Computed(NoirData),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
enum FillStateMean {
    #[default]
    None,
    Accumulating((Option<NoirData>, Option<NoirData>)),
    Computed(NoirData),
}

macro_rules! fill_iterate {
    ($name: ident, $func: ident, $var:ident, $(#[$meta:meta])*) => {
        $(#[$meta])*
        pub fn $name(self) -> Stream<impl Operator<Out = NoirData>>{
            let ($var, stream) = self.shuffle().iterate(
                2,
                FillState::default(),
                |s, state| {
                    s.map(move |v| {
                        if let FillState::Computed($var) = state.get() {
                            v.or($var)
                        } else {
                            v
                        }
                    })
                },
                |$var: &mut Option<NoirData>, v| {
                    v.$func($var, true);
                },
                |a, $var| {
                    match a {
                        FillState::None => *a = FillState::Accumulating($var),
                        FillState::Accumulating($name) => {
                            if $var.is_some(){
                                $var.unwrap().$func($name, true);
                            }
                        }
                        FillState::Computed(_) => {} // final loop
                    }
                },
                |s| {
                    match s {
                        FillState::None => false, // No elements in stream
                        FillState::Accumulating($var) => {
                            *s = FillState::Computed($var.take().unwrap());
                            true
                        }
                        FillState::Computed(_) => false, // terminated
                    }
                },
            );

            $var.for_each(std::mem::drop);
            stream
        }
    };
}

impl<Op> Stream<Op>
where
    Op: Operator<Out = NoirData> + 'static,
{
    /// Drop all the rows that contain None values.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::data_type::{NoirData, NoirType};
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(vec![NoirData::Row(vec![NoirType::Int32(1), NoirType::Int32(2)]), NoirData::Row(vec![NoirType::None(), NoirType::Int32(4)])].into_iter()));
    /// let res = s
    ///     .drop_none()
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![NoirData::Row(vec![NoirType::from(1), NoirType::from(2)])]);
    /// ```
    pub fn drop_none(self) -> Stream<impl Operator<Out = NoirData>> {
        self.filter(move |value| !value.contains_none())
    }

    /// Returns a new stream with the specified columns dropped from each row.
    ///
    /// If the row contains only one column, the row will be converted to a NoirType.
    ///
    /// columns: A vector of column indices to drop.
    /// **Note**: the first column as index 1.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::data_type::{NoirData, NoirType};
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(vec![NoirData::Row(vec![NoirType::Int32(1), NoirType::Int32(2)]), NoirData::Row(vec![NoirType::None(), NoirType::Int32(4)])].into_iter()));
    /// let res = s
    ///     .drop_columns(vec![1])
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![NoirData::NoirType(NoirType::from(2)), NoirData::NoirType(NoirType::from(4))]);
    /// ```
    pub fn drop_columns(self, columns: Vec<usize>) -> Stream<impl Operator<Out = NoirData>> {
        self.map(move |value| match value {
            NoirData::Row(mut row) => {
                let mut i = 0;
                row.retain(|_| {
                    i += 1;
                    !columns.contains(&i)
                });

                if row.len() == 1 {
                    NoirData::NoirType(row[0])
                } else {
                    NoirData::Row(row)
                }
            }
            NoirData::NoirType(_) => value,
        })
    }

    pub fn retain_columns(self, columns: Vec<usize>) -> Stream<impl Operator<Out = NoirData>> {
        self.map(move |value| match value {
            NoirData::Row(mut row) => {
                let mut i = 0;
                row.retain(|_| {
                    i += 1;
                    columns.contains(&i)
                });

                if row.len() == 1 {
                    NoirData::NoirType(row[0])
                } else {
                    NoirData::Row(row)
                }
            }
            NoirData::NoirType(_) => value,
        })
    }

    /// Returns a new `Stream` that replaces missing values in each row with a constant value.
    ///
    /// value: The constant value to replace missing values with.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::data_type::{NoirData, NoirType};
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(vec![NoirData::Row(vec![NoirType::Int32(1), NoirType::Int32(2)]), NoirData::Row(vec![NoirType::None(), NoirType::Int32(4)])].into_iter()));
    /// let res = s
    ///     .fill_constant(NoirType::from(2))
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![NoirData::Row(vec![NoirType::from(1), NoirType::from(2)]), NoirData::Row(vec![NoirType::from(2), NoirType::from(4)])]);
    /// ```
    pub fn fill_constant(self, value: NoirType) -> Stream<impl Operator<Out = NoirData>> {
        self.map(move |data| match data {
            NoirData::Row(mut row) => {
                for v in row.iter_mut() {
                    if v.is_none() {
                        *v = value;
                    }
                }
                NoirData::Row(row)
            }
            NoirData::NoirType(v) => {
                if v.is_none() {
                    NoirData::NoirType(value)
                } else {
                    data
                }
            }
        })
    }

    /// Fills missing data in a stream using a provided function.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::data_type::{NoirData, NoirType};
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(vec![NoirData::Row(vec![NoirType::Int32(1), NoirType::Int32(2)]), NoirData::Row(vec![NoirType::None(), NoirType::Int32(4)])].into_iter()));
    /// let res = s
    ///     .fill_function(|v| {
    ///         let mut row = v.columns().unwrap();
    ///         if row[0].is_none() {
    ///             row[0] = row[1] * 2
    ///         }
    ///         NoirData::Row(row)
    ///     })
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![NoirData::Row(vec![NoirType::from(1), NoirType::from(2)]), NoirData::Row(vec![NoirType::from(8), NoirType::from(4)])]);
    /// ```
    pub fn fill_function<F>(self, f: F) -> Stream<impl Operator<Out = NoirData>>
    where
        F: Fn(NoirData) -> NoirData + Send + Clone + 'static,
    {
        /// Wraps the provided function and checks if it returns None.
        fn wrapper<F>(f: F, value: NoirData) -> NoirData
        where
            F: Fn(NoirData) -> NoirData + Send + Clone + 'static,
        {
            let new = f(value);
            if new.contains_none() {
                panic!("The function passed to fill_function should not return None")
            } else {
                new
            }
        }

        let func = move |value: NoirData| {
            if value.contains_none() {
                wrapper(f.clone(), value)
            } else {
                value
            }
        };

        self.map(func)
    }

    pub fn fill_backward(self) -> Stream<impl Operator<Out = NoirData>> {
        self.replication(crate::Replication::One).rich_map({
            let mut last_values: Option<NoirData> = None;

            move |mut item| match &mut item {
                NoirData::Row(row) => {
                    if last_values.is_none() {
                        last_values = Some(NoirData::Row(vec![NoirType::None(); row.len()]));
                    }

                    let last = last_values.as_mut().unwrap().get_row();

                    for (i, v) in row.iter_mut().enumerate() {
                        if v.is_none() && !last[i].is_none() {
                            *v = last[i];
                        } else if !v.is_na() {
                            last[i] = *v;
                        }
                    }
                    NoirData::Row(row.to_vec())
                }
                NoirData::NoirType(v) => {
                    if !v.is_na() {
                        last_values = Some(item.clone());
                        item
                    } else if v.is_none() && last_values.is_some() {
                        last_values.clone().unwrap()
                    } else {
                        item
                    }
                }
            }
        })
    }

    fill_iterate!(fill_max, max, max,
        /// Fills missing data in a stream using the maximum value in the stream.
        ///
        /// ## Example
        /// ```
        /// # use noir::{StreamEnvironment, EnvironmentConfig};
        /// # use noir::operator::source::IteratorSource;
        /// # use noir::data_type::{NoirData, NoirType};
        /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
        /// let s = env.stream(IteratorSource::new(vec![NoirData::Row(vec![NoirType::Int32(1), NoirType::Int32(2)]), NoirData::Row(vec![NoirType::None(), NoirType::Int32(4)])].into_iter()));
        /// let res = s
        ///     .fill_max()
        ///     .collect_vec();
        ///
        /// env.execute_blocking();
        ///
        /// assert_eq!(res.get().unwrap(), vec![NoirData::Row(vec![NoirType::from(1), NoirType::from(2)]), NoirData::Row(vec![NoirType::from(1), NoirType::from(4)])]);
        /// ```
    );

    /// Fills missing data in a stream using the mean value in the stream.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::data_type::{NoirData, NoirType};
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(vec![NoirData::Row(vec![NoirType::Int32(1), NoirType::Int32(2)]), NoirData::Row(vec![NoirType::None(), NoirType::Int32(4)])].into_iter()));
    /// let res = s
    ///     .fill_mean()
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![NoirData::Row(vec![NoirType::from(1), NoirType::from(2)]), NoirData::Row(vec![NoirType::from(1.0), NoirType::from(4)])]);
    /// ```
    pub fn fill_mean(self) -> Stream<impl Operator<Out = NoirData>> {
        let (mean, stream) = self.shuffle().iterate(
            2,
            FillStateMean::default(),
            |s, state| {
                s.map(move |v| {
                    if let FillStateMean::Computed(mean) = state.get() {
                        v.or(mean)
                    } else {
                        v
                    }
                })
            },
            |(sum, count), v| {
                v.sum_count(sum, count, true);
            },
            |a, item| {
                match a {
                    FillStateMean::None => *a = FillStateMean::Accumulating(item),
                    FillStateMean::Accumulating((s, c)) => {
                        let value = (item.0.unwrap(), item.1.unwrap());
                        NoirData::global_sum_count(s, c, true, value);
                    }
                    FillStateMean::Computed(_) => {} // final loop
                }
            },
            |s| {
                match s {
                    FillStateMean::None => false, // No elements in stream
                    FillStateMean::Accumulating((sum, count)) => {
                        NoirData::mean(sum, count.clone().unwrap(), true);
                        *s = FillStateMean::Computed(sum.clone().unwrap());
                        true
                    }
                    FillStateMean::Computed(_) => false, // terminated
                }
            },
        );

        mean.for_each(std::mem::drop);
        stream
    }

    fill_iterate!(fill_min, min, min,
    /// Fills missing data in a stream using the minimum value in the stream.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::data_type::{NoirData, NoirType};
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(vec![NoirData::Row(vec![NoirType::Int32(1), NoirType::Int32(2)]), NoirData::Row(vec![NoirType::None(), NoirType::Int32(4)])].into_iter()));
    /// let res = s
    ///     .fill_min()
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![NoirData::Row(vec![NoirType::from(1), NoirType::from(2)]), NoirData::Row(vec![NoirType::from(1), NoirType::from(4)])]);
    /// ```
    );

    pub fn fill_forward(self) -> Stream<impl Operator<Out = NoirData>> {
        self.replication(crate::Replication::One)
            .add_operator(FillForward::new)
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FillForward<PreviousOperators>
where
    PreviousOperators: Operator<Out = NoirData>,
{
    prev: PreviousOperators,
    searching: Option<Vec<bool>>,
    buffer: VecDeque<NoirData>,
    to_send: VecDeque<NoirData>,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
}

impl<PreviousOperators> Display for FillForward<PreviousOperators>
where
    PreviousOperators: Operator<Out = NoirData>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Fill_Forward<{}>",
            self.prev,
            std::any::type_name::<NoirData>(),
        )
    }
}

impl<PreviousOperators: Operator<Out = NoirData>> FillForward<PreviousOperators> {
    pub fn new(prev: PreviousOperators) -> Self {
        Self {
            prev,
            searching: None,
            buffer: VecDeque::new(),
            to_send: VecDeque::new(),
            timestamp: None,
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
        }
    }
}

impl<PreviousOperators> Operator for FillForward<PreviousOperators>
where
    PreviousOperators: Operator<Out = NoirData>,
{
    type Out = NoirData;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<NoirData> {
        if !self.to_send.is_empty() {
            return StreamElement::Item(self.to_send.pop_front().unwrap());
        }

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
                StreamElement::Item(item) => match item {
                    NoirData::Row(row) => {
                        if self.searching.is_none() {
                            self.searching = Some(vec![false; row.len()]);
                        }

                        let mut found_none = false;
                        for (i, v) in row.iter().enumerate() {
                            if v.is_none() {
                                self.searching.as_mut().unwrap()[i] = true;
                                found_none = true;
                            } else if !v.is_na() && self.searching.as_ref().unwrap()[i] {
                                self.searching.as_mut().unwrap()[i] = false;

                                for buf in self.buffer.iter_mut() {
                                    if buf.row()[i].is_none() {
                                        buf.get_row()[i] = *v;
                                    }
                                }
                            }
                        }

                        while !self.buffer.is_empty()
                            && !self.buffer.front().unwrap().contains_none()
                        {
                            self.to_send.push_back(self.buffer.pop_front().unwrap());
                        }

                        if found_none {
                            self.buffer.push_back(NoirData::Row(row));
                        } else if !self.to_send.is_empty() {
                            self.to_send.push_back(NoirData::Row(row));
                            return StreamElement::Item(self.to_send.pop_front().unwrap());
                        } else {
                            return StreamElement::Item(NoirData::Row(row));
                        }

                        if !self.to_send.is_empty() {
                            return StreamElement::Item(self.to_send.pop_front().unwrap());
                        }
                    }
                    NoirData::NoirType(v) => {
                        if self.searching.is_none() {
                            self.searching = Some(vec![false]);
                        }

                        if v.is_none() {
                            self.searching.as_mut().unwrap()[0] = true;
                            self.buffer.push_back(item);
                        } else if !v.is_na() {
                            if self.searching.as_ref().unwrap()[0] {
                                self.searching.as_mut().unwrap()[0] = false;

                                while !self.buffer.is_empty() {
                                    self.buffer.pop_front();
                                    self.to_send.push_back(item.clone());
                                }
                            }

                            if !self.to_send.is_empty() {
                                self.to_send.push_back(item.clone());
                                return StreamElement::Item(self.to_send.pop_front().unwrap());
                            } else {
                                return StreamElement::Item(item);
                            }
                        }
                    }
                },
                StreamElement::Timestamped(item, ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    match item {
                        NoirData::Row(row) => {
                            if self.searching.is_none() {
                                self.searching = Some(vec![false; row.len()]);
                            }

                            let mut found_none = false;
                            for (i, v) in row.iter().enumerate() {
                                if v.is_none() {
                                    self.searching.as_mut().unwrap()[i] = true;
                                    found_none = true;
                                } else if !v.is_na() && self.searching.as_ref().unwrap()[i] {
                                    self.searching.as_mut().unwrap()[i] = false;

                                    for buf in self.buffer.iter_mut() {
                                        if buf.row()[i].is_none() {
                                            buf.get_row()[i] = *v;
                                        }
                                    }
                                }
                            }

                            if found_none {
                                self.buffer.push_back(NoirData::Row(row));
                            }

                            while !self.buffer.is_empty()
                                || self.buffer.front().unwrap().contains_none()
                            {
                                self.to_send.push_back(self.buffer.pop_front().unwrap());
                            }

                            if !self.to_send.is_empty() {
                                return StreamElement::Item(self.to_send.pop_front().unwrap());
                            }
                        }
                        NoirData::NoirType(v) => {
                            if self.searching.is_none() {
                                self.searching = Some(vec![false]);
                            }

                            if v.is_none() {
                                self.searching.as_mut().unwrap()[0] = true;
                                self.buffer.push_back(item);
                            } else if !v.is_na() && self.searching.as_ref().unwrap()[0] {
                                self.buffer.pop_front();
                                self.searching.as_mut().unwrap()[0] = false;

                                while !self.buffer.is_empty() {
                                    self.buffer.pop_front();
                                    self.to_send.push_back(item.clone());
                                }

                                return StreamElement::Item(item);
                            }
                        }
                    }
                }
                // this block wont sent anything until the stream ends
                StreamElement::FlushBatch => {}
            }
        }

        if !self.buffer.is_empty() {
            return StreamElement::Item(self.buffer.pop_front().unwrap());
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
            .add_operator(OperatorStructure::new::<NoirData, _>("Fill_Forward"))
    }
}
