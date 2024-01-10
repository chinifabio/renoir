use crate::{
    data_type::{NoirData, NoirType},
    Stream,
};

use super::Operator;

impl<Op> Stream<Op>
where
    Op: Operator<Out = NoirData> + 'static,
{
    pub fn entropy(self, skip_na: bool) -> Stream<impl Operator<Out = NoirData>> {
        self.fold_assoc(
            (None, None::<Vec<usize>>, false),
            move |acc, value| {
                if !acc.2 {
                    acc.2 = value.mode(&mut acc.0, skip_na);
                }
            },
            |acc, value| {
                let (bins_acc, count_acc, _) = acc;
                let bins = value.0;

                if bins_acc.is_none() {
                    *count_acc = Some(vec![0; bins.as_ref().unwrap().len()]);
                    for (i, bin) in bins.as_ref().unwrap().iter().enumerate() {
                        if let Some(b) = bin {
                            for v in b.values() {
                                count_acc.as_mut().unwrap()[i] += v;
                            }
                        }
                    }
                    *bins_acc = bins;
                } else {
                    for (i, bin) in bins.unwrap().into_iter().enumerate() {
                        if let Some(b) = bin {
                            if let Some(b_acc) = &mut bins_acc.as_mut().unwrap()[i] {
                                for (k, v) in b {
                                    if let Some(v_acc) = b_acc.get_mut(&k) {
                                        *v_acc += v;
                                    } else {
                                        b_acc.insert(k, v);
                                    }
                                    count_acc.as_mut().unwrap()[i] += v;
                                }
                            } else {
                                for v in b.values() {
                                    count_acc.as_mut().unwrap()[i] += v;
                                }
                                bins_acc.as_mut().unwrap()[i] = Some(b);
                            }
                        } else {
                            bins_acc.as_mut().unwrap()[i] = None;
                        }
                    }
                }
            },
        )
        .map(|acc| match acc.0 {
            Some(mut bins) => {
                if bins.len() > 1 {
                    let mut result = NoirData::Row(vec![NoirType::NaN(); bins.len()]);
                    for (i, bin) in bins.into_iter().enumerate() {
                        if let Some(mut b) = bin {
                            let mut entropy = NoirType::None();
                            for (_, v) in b.drain() {
                                if entropy.is_none() {
                                    entropy = NoirType::Float32(0.0);
                                }
                                let p = v as f32 / acc.1.as_ref().unwrap()[i] as f32;
                                entropy += NoirType::Float32(-p * p.log2());
                            }
                            result.get_row()[i] = entropy;
                        }
                    }
                    result
                } else if bins[0].is_none() {
                    NoirData::NoirType(NoirType::NaN())
                } else {
                    let mut result = NoirData::NoirType(NoirType::None());
                    for (_, v) in bins.pop().unwrap().unwrap().drain() {
                        if result.get_type().is_none() {
                            *result.get_type() = NoirType::Float32(0.0);
                        }
                        let p = v as f32 / acc.1.as_ref().unwrap()[0] as f32;
                        *result.get_type() += NoirType::Float32(-p * p.log2());
                    }
                    result
                }
            }
            None => NoirData::NoirType(NoirType::None()),
        })
    }
}
