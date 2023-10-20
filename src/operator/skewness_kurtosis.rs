use crate::{data_type::NoirData, Stream};

use super::Operator;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
enum FillState {
    #[default]
    None,
    Accumulating((Option<NoirData>, Option<NoirData>, Option<NoirData>, bool)), //count, mean, m2, found_nan
    Computed((NoirData, NoirData, NoirData)),                                   //count, mean, std
}

macro_rules! skew_kurt {
    ($self: ident, $exp: expr, $skip_na: ident) => {
        $self.shuffle().iterate(
            2,
            FillState::default(),
            move |s, state| {
                s.map(move |v| {
                    if let FillState::Computed((count, mean, std)) = state.get() {
                        let mut skew = None;
                        v.skew_kurt(&mut skew, count, mean, std, $skip_na, $exp);
                        skew.unwrap()
                    } else {
                        v
                    }
                })
            },
            move |(count, mean, m2, found_nan), v| match found_nan {
                true => {}
                false => {
                    *found_nan = v.welford(count, mean, m2, $skip_na);
                }
            },
            move |state, item| match state {
                FillState::None => *state = FillState::Accumulating(item),
                FillState::Accumulating(acc) => {
                    if !acc.3 && item.0.is_some() && item.1.is_some() && item.2.is_some() {
                        let value = (item.0.unwrap(), item.1.unwrap(), item.2.unwrap());

                        acc.3 = NoirData::chen(&mut acc.0, &mut acc.1, &mut acc.2, $skip_na, value);
                    }
                }
                FillState::Computed(_) => {}
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
                            if v_m.is_na() {
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
                            true
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
                                if mean[i].is_na() {
                                    result_count.push(mean[i]);
                                    result_mean.push(mean[i]);
                                    result_std.push(mean[i]);
                                } else {
                                    result_count.push(count[i]);
                                    result_mean.push(mean[i]);
                                    result_std.push((m2[i] / (*v - 1)).sqrt());
                                }
                            }
                            *s = FillState::Computed((
                                NoirData::Row(result_count),
                                NoirData::Row(result_mean),
                                NoirData::Row(result_std),
                            ));
                            true
                        }
                        _ => panic!("Fatal error in Entropy"),
                    },
                    FillState::Computed(_) => false, // terminated
                }
            },
        )
    };
}

impl<Op> Stream<NoirData, Op>
where
    Op: Operator<NoirData> + 'static,
{
    pub fn skewness(self, skip_na: bool) -> Stream<NoirData, impl Operator<NoirData>> {
        let (res, stream) = skew_kurt!(self, 3, skip_na);

        res.for_each(std::mem::drop);

        stream
            .fold(None, move |sum, v| {
                v.sum(sum, skip_na);
            })
            .map(|v| v.unwrap())
    }

    pub fn kurtosis(self, skip_na: bool) -> Stream<NoirData, impl Operator<NoirData>> {
        let (res, stream) = skew_kurt!(self, 4, skip_na);

        res.for_each(std::mem::drop);

        stream
            .fold(None, move |sum, v| {
                v.sum(sum, skip_na);
            })
            .map(|v| v.unwrap())
    }
}
