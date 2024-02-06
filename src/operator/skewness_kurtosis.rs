use crate::data_type::noir_data::NoirData;
use crate::Stream;

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

impl<Op> Stream<Op>
where
    Op: Operator<Out = NoirData> + 'static,
{
    pub fn skewness(self, skip_na: bool) -> Stream<impl Operator<Out = NoirData>> {
        let (res, stream) = skew_kurt!(self, 3, skip_na);

        res.for_each(std::mem::drop);

        stream
            .fold(None, move |sum, v| {
                v.sum(sum, skip_na);
            })
            .map(|v| v.unwrap())
    }

    pub fn kurtosis(self, skip_na: bool) -> Stream<impl Operator<Out = NoirData>> {
        let (res, stream) = skew_kurt!(self, 4, skip_na);

        res.for_each(std::mem::drop);

        stream
            .fold(None, move |sum, v| {
                v.sum(sum, skip_na);
            })
            .map(|v| v.unwrap())
    }

    pub fn kurtosis_unbiased(self, skip_nan: bool) -> Stream<impl Operator<Out = NoirData>> {
        self.fold_assoc(
            (None, None, None, None, None, false),
            move |acc, v| {
                if !acc.5 {
                    acc.5 = v.count_kumulant_4(
                        &mut acc.0, &mut acc.1, &mut acc.2, &mut acc.3, &mut acc.4, skip_nan,
                    )
                }
            },
            move |acc, v| {
                if !acc.5 {
                    acc.5 = NoirData::global_count_kumulant_4(
                        &mut acc.0,
                        &mut acc.1,
                        &mut acc.2,
                        &mut acc.3,
                        &mut acc.4,
                        skip_nan,
                        (
                            v.0.unwrap(),
                            v.1.unwrap(),
                            v.2.unwrap(),
                            v.3.unwrap(),
                            v.4.unwrap(),
                        ),
                    )
                }
            },
        )
        .map(|v| {
            let count = v.0.unwrap();
            let s1 = v.1.unwrap();
            let s2 = v.2.unwrap();
            let s3 = v.3.unwrap();
            let s4 = v.4.unwrap();

            match (count, s1, s2, s3, s4) {
                (
                    NoirData::Row(count),
                    NoirData::Row(s1),
                    NoirData::Row(s2),
                    NoirData::Row(s3),
                    NoirData::Row(s4),
                ) => {
                    let mut kurt = Vec::with_capacity(count.len());
                    for (i, v) in count.into_iter().enumerate() {
                        if !v.is_na()
                            && !s1[i].is_na()
                            && !s2[i].is_na()
                            && !s3[i].is_na()
                            && !s4[i].is_na()
                        {
                            let v = f64::from(v);
                            let s1 = f64::from(s1[i]);
                            let s2 = f64::from(s2[i]);
                            let s3 = f64::from(s3[i]);
                            let s4 = f64::from(s4[i]);

                            let k4 = (-6.0 * s1.powi(4) + 12.0 * v * s1.powi(2) * s2
                                - 3.0 * v * (v - 1.0) * s2.powi(2)
                                - 4.0 * v * (v + 1.0) * s1 * s3
                                + v.powi(2) * (v + 1.0) * s4)
                                / (v * (v - 1.0) * (v - 2.0) * (v - 3.0));
                            let k2 = (v * s2 - s1.powi(2)) / (v * (v - 1.0));

                            kurt.push(crate::data_type::noir_type::NoirType::Float32(
                                (k4 / k2.powi(2)) as f32,
                            ));
                        } else if v.is_nan()
                            || s1[i].is_nan()
                            || s2[i].is_nan()
                            || s3[i].is_nan()
                            || s4[i].is_nan()
                        {
                            kurt.push(crate::data_type::noir_type::NoirType::NaN())
                        } else {
                            kurt.push(crate::data_type::noir_type::NoirType::None())
                        }
                    }

                    NoirData::Row(kurt)
                }
                (
                    NoirData::NoirType(count),
                    NoirData::NoirType(s1),
                    NoirData::NoirType(s2),
                    NoirData::NoirType(s3),
                    NoirData::NoirType(s4),
                ) => {
                    if !count.is_na() && !s1.is_na() && !s2.is_na() && !s3.is_na() && !s4.is_na() {
                        let count = f64::from(count);
                        let s1 = f64::from(s1);
                        let s2 = f64::from(s2);
                        let s3 = f64::from(s3);
                        let s4 = f64::from(s4);

                        let k4 = (-6.0 * s1.powi(4) + 12.0 * count * s1.powi(2) * s2
                            - 3.0 * count * (count - 1.0) * s2.powi(2)
                            - 4.0 * count * (count + 1.0) * s1 * s3
                            + count.powi(2) * (count + 1.0) * s4)
                            / (count * (count - 1.0) * (count - 2.0) * (count - 3.0));

                        let k2 = (count * s2 - s1.powi(2)) / (count * (count - 1.0));

                        NoirData::NoirType(crate::data_type::noir_type::NoirType::Float32(
                            (k4 / k2.powi(2)) as f32,
                        ))
                    } else if count.is_nan()
                        || s1.is_nan()
                        || s2.is_nan()
                        || s3.is_nan()
                        || s4.is_nan()
                    {
                        NoirData::NoirType(crate::data_type::noir_type::NoirType::NaN())
                    } else {
                        NoirData::NoirType(crate::data_type::noir_type::NoirType::None())
                    }
                }
                _ => panic!("Fatal error in Kurtosis"),
            }
        })
    }
}
