use crate::{data_type::NoirData, Stream};

use super::Operator;

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
        self.fold_assoc(
            (None, None, None, false),
            move |acc, item| {
                if !acc.3 {
                    acc.3 = item.welford(&mut acc.0, &mut acc.1, &mut acc.2, skip_na);
                }
            },
            move |acc, item| {
                let (count, mean, m2, found_nan) = acc;
                if !*found_nan {
                    *found_nan = NoirData::chen(
                        count,
                        mean,
                        m2,
                        skip_na,
                        (item.0.unwrap(), item.1.unwrap(), item.2.unwrap()),
                    );
                }
            },
        )
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
