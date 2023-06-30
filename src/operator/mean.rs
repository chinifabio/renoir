use std::ops::{AddAssign, Div};

use super::{fold::Fold, Data, ExchangeData, Operator};
use crate::{Replication, Stream};

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
