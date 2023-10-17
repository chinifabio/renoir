use crate::{data_type::NoirData, Stream};

use super::Operator;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
enum FillState {
    #[default]
    None,
    Accumulating((Option<NoirData>, Option<NoirData>, Option<NoirData>, bool)), //count, mean, m2, found_nan
    Computed((NoirData, NoirData, NoirData)), //count, mean, variance
}

impl<Op> Stream<NoirData, Op>
where
    Op: Operator<NoirData> + 'static,
{
    pub fn skewness(self, skip_na: bool) -> Stream<NoirData, impl Operator<NoirData>> {
        let (res, stream) = self.shuffle().iterate(
            2,
            FillState::default(),
            move |s, state| {
                s.fold((None, false), move |acc, v| {
                    if let FillState::Computed((count, mean, std)) = state.get() {
                        if !acc.1 {
                            acc.1 = v.skewness(&mut acc.0,count, mean, std,  skip_na);
                        }
                    }
                })
                .map(|v| {
                    v.0.unwrap()
                })
            },
            move |(count, mean, m2, found_nan), v| match found_nan {
                true => {}
                false => *found_nan = v.welford(count, mean, m2, skip_na),
            },
            move |state, item| match state {
                FillState::None => *state = FillState::Accumulating(item),
                FillState::Accumulating(acc) => {
                    if !acc.3 {
                        let value = (item.0.unwrap(), item.1.unwrap(), item.2.unwrap());

                        acc.3 = NoirData::chen(&mut acc.0, &mut acc.1, &mut acc.2, skip_na, value);
                    }
                }
                FillState::Computed(_) => todo!(),
            },
            |s| {
                match s {
                    FillState::None => false, // No elements in stream
                    FillState::Accumulating((count, mean, m2, _)) => match (count, mean, m2) {
                        (
                            Some(NoirData::NoirType(v_c)),
                            Some(NoirData::NoirType(v_m)),
                            Some(NoirData::NoirType(v_m2)),
                        ) => {
                            if v_c.is_na() || v_m2.is_na() || v_m.is_na() {
                                *s = FillState::Computed((
                                    NoirData::NoirType(*v_m),
                                    NoirData::NoirType(*v_m),
                                    NoirData::NoirType(*v_m),
                                ));
                            } else {
                                *s = FillState::Computed((
                                    NoirData::NoirType(*v_c),
                                    NoirData::NoirType(*v_m),
                                    NoirData::NoirType((*v_m2 / (*v_c - 1)).sqrt()),
                                ))
                            }
                            return true;
                        }
                        (
                            Some(NoirData::Row(count)),
                            Some(NoirData::Row(mean)),
                            Some(NoirData::Row(m2)),
                        ) => {
                            let mut result_count = Vec::with_capacity(count.len());
                            let mut result_mean = Vec::with_capacity(count.len());
                            let mut result_std = Vec::with_capacity(count.len());
                            for (i, v) in count.iter().enumerate() {
                                if v.is_na() || m2[i].is_na() || mean[i].is_na() {
                                    result_count.push(mean[i]);
                                    result_mean.push(mean[i]);
                                    result_std.push(mean[i]);
                                } else {
                                    result_count.push(count[i]);
                                    result_mean.push(mean[i]);
                                    result_std.push((m2[i] / (*v - 1)).sqrt());
                                }
                            }
                            return true;
                        }
                        _ => panic!("Fatal error in Entropy"),
                    },
                    FillState::Computed(_) => false, // terminated
                }
            },
        );

        res.for_each(std::mem::drop);
        stream
    }
}
