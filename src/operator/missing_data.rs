use crate::{Stream, data_type::{NoirData, NoirType}};

use super::Operator;



impl<Op> Stream<NoirData, Op>
where
    Op: Operator<NoirData> + 'static,
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
    pub fn drop_none(self) -> Stream<NoirData, impl Operator<NoirData>> {
        self.filter(move |value| {
            if value.contains_none() {
                false
            } else {
                true
            }
        })
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
    pub fn drop_columns(self, columns: Vec<usize>) -> Stream<NoirData, impl Operator<NoirData>> {
        self.map(move |value| {
            match value{
                NoirData::Row(mut row) => {
                    let mut i = 0;
                    row.retain(|_| {
                        i+=1;
                        !columns.contains(&i)
                    });

                    if row.len() == 1 {
                        NoirData::NoirType(NoirType::from(row[0]))
                    } else{
                        NoirData::Row(row)
                    }
                }
                NoirData::NoirType(_) => value,
            }
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
    pub fn fill_constant(self, value: NoirType) -> Stream<NoirData, impl Operator<NoirData>> {
        self.map(move |data| {
            match data{
                NoirData::Row(row) => {
                    let new = row.iter().map(|v| {
                        if v.is_none() {
                            value
                        }else {
                            *v
                        }
                    }).collect::<Vec<_>>();
                    NoirData::Row(new)
                }
                NoirData::NoirType(v) => {
                    if v.is_none() {
                        NoirData::NoirType(value)
                    }else{
                        data
                    }
                },
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
    pub fn fill_function<F>(self, f: F) -> Stream<NoirData, impl Operator<NoirData>>
    where
        F: Fn(NoirData) -> NoirData + Send + Clone + 'static,
    {
        /// Wraps the provided function and checks if it returns None.
        fn wrapper<F>(f: F, value: NoirData)-> NoirData
        where
            F: Fn(NoirData) -> NoirData + Send + Clone + 'static,
        {
            let new = f(value);
            if new.contains_none() {
                panic!("The function passed to fill_function should not return None")
            }else{
                new
            }
        }

        let func = move |value: NoirData| wrapper(f.clone(), value);

        self.map(func)
    }
}


