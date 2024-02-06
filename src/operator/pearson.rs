use crate::data_type::noir_data::NoirData;
use crate::data_type::noir_type::NoirType;
use crate::Stream;

use super::Operator;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
enum FillState {
    #[default]
    None,
    Accumulating((Option<NoirData>, Option<NoirData>, Option<NoirData>, bool)), //count, mean, m2, found_nan
    Computed((NoirData, NoirData, NoirData)),                                   //count, mean, std
}

impl<Op> Stream<Op>
where
    Op: Operator<Out = NoirData> + 'static,
{
    pub fn pearson(self, columns: [usize; 2]) -> Stream<impl Operator<Out = NoirData>> {
        let (res, stream) = self.retain_columns(columns.to_vec()).shuffle().iterate(
            2,
            FillState::default(),
            move |s, state| {
                s.map(move |v| {
                    if let FillState::Computed((count, mean, std)) = state.get() {
                        if count.type_().is_na() {
                            NoirData::NoirType(count.clone().to_type())
                        } else {
                            let mut corr = None;
                            v.pearson(&mut corr, count, mean, std);
                            corr.unwrap()
                        }
                    } else {
                        v
                    }
                })
            },
            move |(count, mean, m2, found_nan), v| match found_nan {
                true => {}
                false => {
                    *found_nan = v.welford(count, mean, m2, false);
                }
            },
            move |state, item| match state {
                FillState::None => *state = FillState::Accumulating(item),
                FillState::Accumulating(acc) => {
                    if !acc.3 && item.0.is_some() && item.1.is_some() && item.2.is_some() {
                        let value = (item.0.unwrap(), item.1.unwrap(), item.2.unwrap());

                        acc.3 = NoirData::chen(&mut acc.0, &mut acc.1, &mut acc.2, false, value);
                    }
                }
                FillState::Computed(_) => {}
            },
            |s| {
                match s {
                    FillState::None => false, // No elements in stream
                    FillState::Accumulating((count, mean, m2, _)) => match (count, mean, m2) {
                        (
                            Some(NoirData::Row(count)),
                            Some(NoirData::Row(mean)),
                            Some(NoirData::Row(m2)),
                        ) => {
                            if mean[0].is_nan() || mean[1].is_nan() {
                                *s = FillState::Computed((
                                    NoirData::NoirType(NoirType::NaN()),
                                    NoirData::Row(vec![NoirType::NaN(), NoirType::NaN()]),
                                    NoirData::Row(vec![NoirType::NaN(), NoirType::NaN()]),
                                ));
                            } else if mean[0].is_none() || mean[1].is_none() {
                                *s = FillState::Computed((
                                    NoirData::NoirType(NoirType::None()),
                                    NoirData::Row(vec![NoirType::None(), NoirType::None()]),
                                    NoirData::Row(vec![NoirType::None(), NoirType::None()]),
                                ));
                            } else {
                                *s = FillState::Computed((
                                    NoirData::NoirType(count[0]),
                                    NoirData::Row(vec![mean[0], mean[1]]),
                                    NoirData::Row(vec![
                                        (m2[0] / (count[0] - 1)).sqrt(),
                                        (m2[1] / (count[0] - 1)).sqrt(),
                                    ]),
                                ));
                            }
                            true
                        }
                        _ => panic!("Fatal error in Pearson"),
                    },
                    FillState::Computed(_) => false, // terminated
                }
            },
        );

        res.for_each(std::mem::drop);

        stream
            .fold(None, move |sum, v| {
                v.sum(sum, false);
            })
            .map(|v| v.unwrap())
    }
}
