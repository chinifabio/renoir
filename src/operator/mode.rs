use crate::data_type::noir_data::NoirData;
use crate::data_type::noir_type::NoirType;
use crate::Stream;

use super::Operator;

impl<Op> Stream<Op>
where
    Op: Operator<Out = NoirData> + 'static,
{
    pub fn mode(self, skip_na: bool) -> Stream<impl Operator<Out = Vec<NoirData>>> {
        self.fold_assoc(
            (None, false),
            move |acc, value| {
                if !acc.1 {
                    acc.1 = value.mode(&mut acc.0, skip_na);
                }
            },
            |acc, value| {
                let bins_acc = &mut acc.0;
                let bins = value.0;

                if bins_acc.is_none() {
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
                                }
                            } else {
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
                    let mut result = vec![NoirData::NoirType(NoirType::NaN()); bins.len()];
                    for (i, bin) in bins.into_iter().enumerate() {
                        if let Some(mut b) = bin {
                            let mut max: usize = 0;
                            let mut mode_values = vec![];
                            for (k, v) in b.drain() {
                                match v.cmp(&max) {
                                    std::cmp::Ordering::Less => {}
                                    std::cmp::Ordering::Equal => {
                                        mode_values.push(NoirType::Int32(k))
                                    }
                                    std::cmp::Ordering::Greater => {
                                        max = v;
                                        mode_values = vec![NoirType::Int32(k)];
                                    }
                                }
                            }
                            if max == 1 || max == 0 {
                                result[i] = NoirData::NoirType(NoirType::None());
                            } else {
                                result[i] = NoirData::Row(mode_values);
                            }
                        }
                    }
                    result
                } else {
                    let mut max = 0;
                    if bins[0].is_none() {
                        vec![NoirData::NoirType(NoirType::NaN())]
                    } else {
                        let mut result = vec![];
                        for (k, v) in bins.pop().unwrap().unwrap().drain() {
                            match v.cmp(&max) {
                                std::cmp::Ordering::Less => {}
                                std::cmp::Ordering::Equal => result.push(NoirType::Int32(k)),
                                std::cmp::Ordering::Greater => {
                                    max = v;
                                    result = vec![NoirType::Int32(k)];
                                }
                            }
                        }
                        if max == 1 || max == 0 {
                            vec![NoirData::NoirType(NoirType::None())]
                        } else {
                            vec![NoirData::Row(result)]
                        }
                    }
                }
            }
            None => vec![NoirData::NoirType(NoirType::None())],
        })
    }
}
