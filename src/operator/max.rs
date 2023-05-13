use crate::Stream;
use super::{Operator, ExchangeData, Data, fold::Fold};

impl<I, Op> Stream<I, Op>
where
    I: Data,
    Op: Operator<I> + 'static,
{

    /// Reduce the stream into a stream that emits a single value which is the max value of the stream.
    ///
    /// The reducing operator consists in scanning the stream and keep track of the max value.
    ///
    /// The "get_value" function is used to access the values that will be compered to the current maximun.
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
    pub fn max<O, F>(self, get_value:F) -> Stream<I, impl Operator<I>>
    where
        O: Ord,
        F: Fn(&I) -> O + Clone + Send + 'static,
        I: ExchangeData
    {

        self.max_parallelism(1)
            .add_operator(|prev| 
                Fold::new(prev, None, move |acc: &mut Option<I>, b: I| {
                *acc = Some(
                    if let Some(a) = acc.take() {
                        if get_value(&b)>get_value(&a) {b} else {a} 
                    } 
                    else { b }
                )
            })).map(|value: Option<I>| value.unwrap())
    }
}
