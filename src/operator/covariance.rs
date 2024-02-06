use crate::data_type::noir_data::NoirData;
use crate::data_type::noir_type::NoirType;
use crate::Stream;

use super::Operator;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
enum FillState {
    #[default]
    None,
    Accumulating((Option<NoirData>, Option<NoirData>, bool)), //count, sum, found_nan
    Computed((NoirData, NoirData)),                           //count, mean
}

impl<Op> Stream<Op>
where
    Op: Operator<Out = NoirData> + 'static,
{
    pub fn covariance(self, columns: [usize; 2]) -> Stream<impl Operator<Out = NoirData>> {
        let (state, stream) = self.retain_columns(columns.to_vec()).shuffle().iterate(
            2,
            FillState::default(),
            move |s, state| {
                s.map(move |v| {
                    if let FillState::Computed((count, mean)) = state.get() {
                        if count.type_().is_na() {
                            NoirData::NoirType(count.clone().to_type())
                        } else {
                            let mut cov = None;
                            v.covariance(&mut cov, count, mean);
                            cov.unwrap()
                        }
                    } else {
                        v
                    }
                })
            },
            move |(count, sum, found_nan), v| match found_nan {
                true => {}
                false => {
                    *found_nan = v.sum_count(sum, count, false);
                }
            },
            move |state, item| match state {
                FillState::None => *state = FillState::Accumulating(item),
                FillState::Accumulating(acc) => {
                    if !acc.2 && item.0.is_some() && item.1.is_some() {
                        let value = (item.1.unwrap(), item.0.unwrap());

                        acc.2 = NoirData::global_sum_count(&mut acc.1, &mut acc.0, false, value);
                    }
                }
                FillState::Computed(_) => {}
            },
            |s| {
                match s {
                    FillState::None => false, // No elements in stream
                    FillState::Accumulating((count, sum, _)) => match (count, sum) {
                        (Some(NoirData::Row(count)), Some(NoirData::Row(sum))) => {
                            if sum[0].is_nan() || sum[1].is_nan() {
                                *s = FillState::Computed((
                                    NoirData::NoirType(NoirType::NaN()),
                                    NoirData::Row(vec![NoirType::NaN(), NoirType::NaN()]),
                                ));
                            } else if sum[0].is_none() || sum[1].is_none() {
                                *s = FillState::Computed((
                                    NoirData::NoirType(NoirType::None()),
                                    NoirData::Row(vec![NoirType::None(), NoirType::None()]),
                                ));
                            } else {
                                *s = FillState::Computed((
                                    NoirData::NoirType(count[0]),
                                    NoirData::Row(vec![sum[0] / count[0], sum[1] / count[0]]),
                                ));
                            }
                            true
                        }
                        _ => panic!("Fatal error in Covariance"),
                    },
                    FillState::Computed(_) => false, // terminated
                }
            },
        );

        state.for_each(std::mem::drop);

        stream
            .fold(None, move |sum, v| {
                v.sum(sum, false);
            })
            .map(|v| v.unwrap())
    }
}
