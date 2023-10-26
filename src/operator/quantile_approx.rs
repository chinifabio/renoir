use crate::{
    data_type::{NoirData, NoirType},
    Stream,
};

use super::Operator;

impl<Op> Stream<NoirData, Op>
where
    Op: Operator<NoirData> + 'static,
{
    pub fn ckms(self, skip_na: bool) -> Stream<NoirData, impl Operator<NoirData>> {
        self.fold_assoc(
            (None, false),
            move |acc, v| {
                if !acc.1 {
                    acc.1 = v.ckms(&mut acc.0, skip_na);
                }
            },
            move |acc, v| {
                if !acc.1 {
                    acc.1 = v.1;

                    if acc.0.is_none() {
                        acc.0 = v.0;
                    } else if let (Some(acc), Some(item)) = (acc.0.as_mut(), v.0.as_ref()) {
                        for (q, i) in acc.iter_mut().zip(item.iter()) {
                            match (q.as_mut(), i) {
                                (None, None) => {}
                                (None, Some(b)) => *q = Some(b.clone()),
                                (Some(_), None) => *q = None,
                                (Some(a), Some(b)) => *a += b.clone(),
                            }
                        }
                    }
                }
            },
        )
        .map(|v| {
            let quantiles = v.0.unwrap();
            if quantiles.len() > 1 {
                let mut result = Vec::with_capacity(quantiles.len());
                for q in quantiles.iter() {
                    if q.is_none() {
                        result.push(NoirType::NaN());
                    } else if q.as_ref().unwrap().count() == 0 {
                        result.push(NoirType::None());
                    } else {
                        let quantile = q.as_ref().unwrap().query(0.5).unwrap().1;
                        result.push(quantile);
                    }
                }
                NoirData::Row(result)
            } else {
                let q = quantiles[0].as_ref();

                if let Some(quantiles) = q {
                    if quantiles.count() == 0 {
                        NoirData::NoirType(NoirType::None())
                    } else {
                        NoirData::NoirType(quantiles.query(0.5).unwrap().1)
                    }
                } else {
                    NoirData::NoirType(NoirType::NaN())
                }
            }
        })
    }

    pub fn gk(self, skip_na: bool) -> Stream<NoirData, impl Operator<NoirData>> {
        self.fold_assoc(
            (None, false),
            move |acc, v| {
                if !acc.1 {
                    acc.1 = v.gk(&mut acc.0, skip_na);
                }
            },
            move |acc, v| {
                if !acc.1 {
                    acc.1 = v.1;

                    if acc.0.is_none() {
                        acc.0 = v.0;
                    } else if let (Some(acc), Some(item)) = (acc.0.as_mut(), v.0.as_ref()) {
                        for (q, i) in acc.iter_mut().zip(item.iter()) {
                            match (q.as_mut(), i) {
                                (None, None) => {}
                                (None, Some(b)) => *q = Some(b.clone()),
                                (Some(_), None) => *q = None,
                                (Some(a), Some(b)) => *a += b.clone(),
                            }
                        }
                    }
                }
            },
        )
        .map(|v| {
            let quantiles = v.0.unwrap();
            if quantiles.len() > 1 {
                let mut result = Vec::with_capacity(quantiles.len());
                for q in quantiles.iter() {
                    if q.is_none() {
                        result.push(NoirType::NaN());
                    } else if q.as_ref().unwrap().n() == 0 {
                        result.push(NoirType::None());
                    } else {
                        let quantile = q.as_ref().unwrap().quantile(0.5);
                        result.push(*quantile);
                    }
                }
                NoirData::Row(result)
            } else {
                let q = quantiles[0].as_ref();
                if let Some(quantiles) = q {
                    if quantiles.n() == 0 {
                        NoirData::NoirType(NoirType::None())
                    } else {
                        NoirData::NoirType(*(quantiles.quantile(0.5)))
                    }
                } else {
                    NoirData::NoirType(NoirType::NaN())
                }
            }
        })
    }

    pub fn p2(self, skip_na: bool) -> Stream<NoirData, impl Operator<NoirData>> {
        self.fold((None, false), move |acc, v| {
            if !acc.1 {
                acc.1 = v.p2(&mut acc.0, skip_na);
            }
        })
        .map(|v| {
            let quantiles = v.0.unwrap();
            if quantiles.len() > 1 {
                let mut result = Vec::with_capacity(quantiles.len());
                for q in quantiles.iter() {
                    if q.is_none() {
                        result.push(NoirType::NaN());
                    } else if q.as_ref().unwrap().is_empty() {
                        result.push(NoirType::None());
                    } else {
                        let quantile = q.as_ref().unwrap().quantile();
                        result.push(NoirType::from(quantile as f32));
                    }
                }
                NoirData::Row(result)
            } else {
                let q = quantiles[0].as_ref();
                if let Some(quantile) = q {
                    if quantile.is_empty() {
                        NoirData::NoirType(NoirType::None())
                    } else {
                        NoirData::NoirType(NoirType::from(quantile.quantile() as f32))
                    }
                } else {
                    NoirData::NoirType(NoirType::NaN())
                }
            }
        })
    }
}
