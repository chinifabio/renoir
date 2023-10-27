use super::{fold::Fold, Data, ExchangeData, Operator};
use crate::data_type::NoirType;
use crate::{data_type::NoirData, Replication, Stream};
use std::ops::{AddAssign, Div};

impl<I, Op> Stream<I, Op>
where
    I: Data,
    Op: Operator<I> + 'static,
{
    /// Find the average of the values of the items in the stream.
    ///
    /// The value to average is obtained with `get_value`.
    ///
    /// **Note**: the type of the result does not have to be a number, any type that implements
    /// `AddAssign` and can be divided by `f64` is accepted.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// let res = s
    ///     .mean(|&n| n as f64)
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![4.5]);
    /// ```
    pub fn mean<O, F>(self, get_value: F) -> Stream<O, impl Operator<O>>
    where
        F: Fn(&I) -> O + Clone + Send + Copy + 'static,
        O: ExchangeData + AddAssign + Div<f64, Output = O>,
    {
        self.add_operator(|prev| {
            Fold::new(prev, (None, 0usize), move |(sum, count), value| {
                *count += 1;
                match sum {
                    Some(sum) => *sum += get_value(&value),
                    None => *sum = Some(get_value(&value)),
                }
            })
        })
        .replication(Replication::One)
        .add_operator(|prev| {
            Fold::new(
                prev,
                (None, 0usize),
                move |(sum, count), (local_sum, local_count)| {
                    *count += local_count;
                    match sum {
                        Some(sum) => {
                            if let Some(local_sum) = local_sum {
                                *sum += local_sum;
                            }
                        }
                        None => *sum = local_sum,
                    }
                },
            )
        })
        .map(|(sum, count)| sum.unwrap() / (count as f64))
    }
}

fn mean(
    sum_total: &mut Option<NoirData>,
    counts: &mut Option<Vec<usize>>,
    found_nan: &mut bool,
    skip_na: bool,
    mut count_opt: Option<Vec<usize>>,
    mut value_opt: Option<NoirData>,
) {
    if let Some(value) = value_opt.take() {
        if let Some(count) = count_opt.take() {
            match value {
                NoirData::Row(row) => {
                    if !*found_nan {
                        // if we haven't found a NaN yet, update the sum.
                        if sum_total.is_none() {
                            // if the sum is None, initialize it.
                            *sum_total = Some(NoirData::Row(vec![NoirType::None(); row.len()]));
                            *counts = Some(vec![0; row.len()]);
                        }

                        match sum_total.as_mut().unwrap() {
                            NoirData::Row(r) => {
                                let mut all_nan = true;
                                for (i, v) in row.into_iter().enumerate() {
                                    // for each column, update the corrispondent sum.
                                    if !r[i].is_nan() {
                                        if !v.is_na() {
                                            all_nan = false;
                                            counts.as_mut().unwrap()[i] += count[i];
                                            if r[i].is_none() {
                                                // if the sum is None, set it to the item.
                                                r[i] = v;
                                            } else {
                                                // if the sum is not None, add the item to the sum.
                                                r[i] += v;
                                            }
                                        } else {
                                            // item is a NaN, check if we skip them.
                                            if !skip_na {
                                                // if we don't skip them, set the current sum to NaN.
                                                r[i] = v;
                                            } else {
                                                all_nan = false
                                            }
                                        }
                                    }
                                }
                                *found_nan = all_nan;
                            }
                            NoirData::NoirType(_) => panic!("Mismatched types in Stream"),
                        }
                    }
                }
                NoirData::NoirType(item) => {
                    if !*found_nan {
                        // if we haven't found a NaN yet, update the sum.
                        if sum_total.is_none() {
                            // if the sum is None, initialize it.
                            *sum_total = Some(NoirData::NoirType(NoirType::None()));
                            *counts = Some(vec![0]);
                        }
                        match sum_total.as_ref().unwrap() {
                            NoirData::Row(_) => panic!("Mismatched types in Stream"),
                            NoirData::NoirType(sum) => {
                                // check if the item is a NaN.
                                if !item.is_na() {
                                    counts.as_mut().unwrap()[0] += count[0];
                                    if sum.is_none() {
                                        // if the sum is None, set it to the item.
                                        *sum_total = Some(NoirData::NoirType(item));
                                    } else {
                                        // if the sum is not None, add the item to the sum.
                                        *sum_total = Some(NoirData::NoirType(item + sum));
                                    }
                                } else if !skip_na {
                                    // if we don't skip them, set the sum to NaN.
                                    *sum_total = Some(NoirData::NoirType(item));
                                    *found_nan = true;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<Op> Stream<NoirData, Op>
where
    Op: Operator<NoirData> + 'static,
{
    /// Find the average of the values of the items in the NoirData stream.
    ///
    /// skip_na: if true, NaN and None values will not be considered, otherwise they will be considered as the mean value.
    ///
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::data_type::{NoirData, NoirType};
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(vec![NoirData::Row(vec![NoirType::Int32(1), NoirType::Int32(2)]), NoirData::Row(vec![NoirType::Int32(3), NoirType::Int32(4)])].into_iter()));
    /// let res = s
    ///     .mean_noir_data(true)
    ///     .collect_vec();
    ///
    /// env.execute_blocking();
    ///
    /// assert_eq!(res.get().unwrap(), vec![NoirData::Row(vec![NoirType::from(2.0), NoirType::from(3.0)])]);
    /// ```
    pub fn mean_noir_data(self, skip_na: bool) -> Stream<NoirData, impl Operator<NoirData>> {
        self.fold_assoc(
            (None::<NoirData>, None::<Vec<usize>>, false),
            move |(sum_total, counts, found_nan), value| {
                mean(
                    sum_total,
                    counts,
                    found_nan,
                    skip_na,
                    Some(vec![1; value.len()]),
                    Some(value),
                )
            },
            move |(sum_total, counts, found_nan), (local_value, local_count, _)| {
                mean(
                    sum_total,
                    counts,
                    found_nan,
                    skip_na,
                    local_count,
                    local_value,
                )
            },
        )
        .map(|(sum, count, _)| match sum {
            Some(NoirData::Row(mut row)) => {
                for (i, v) in row.iter_mut().enumerate() {
                    if !v.is_nan() && !v.is_none() {
                        *v = *v / count.as_ref().unwrap()[i];
                    }
                }
                NoirData::Row(row)
            }
            Some(NoirData::NoirType(item)) => {
                if !item.is_nan() && !item.is_none() {
                    NoirData::NoirType(item / count.unwrap()[0])
                } else {
                    NoirData::NoirType(item)
                }
            }
            None => panic!("No sum found"),
        })
    }
}
