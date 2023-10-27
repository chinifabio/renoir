use crate::{
    data_type::{NoirData, NoirType},
    Stream,
};

use super::Operator;

impl<Op> Stream<NoirData, Op>
where
    Op: Operator<NoirData> + 'static,
{
    pub fn ckms(
        self,
        quantile: f64,
        error: f64,
        skip_na: bool,
    ) -> Stream<NoirData, impl Operator<NoirData>> {
        self.fold_assoc(
            (None, false),
            move |acc, v| {
                if !acc.1 {
                    acc.1 = v.ckms(&mut acc.0, error, skip_na);
                }
            },
            move |acc, v| {
                if !acc.1 {
                    acc.1 = v.1;

                    if acc.0.is_none() {
                        acc.0 = v.0;
                    } else if let (Some(acc), Some(item)) = (acc.0.as_mut(), v.0) {
                        for (q, i) in acc.iter_mut().zip(item.into_iter()) {
                            match (q.as_mut(), i) {
                                (None, None) => {}
                                (None, Some(b)) => *q = Some(b),
                                (Some(_), None) => *q = None,
                                (Some(a), Some(b)) => *a += b,
                            }
                        }
                    }
                }
            },
        )
        .map(move |v| {
            let quantiles = v.0.unwrap();
            if quantiles.len() > 1 {
                let mut result = Vec::with_capacity(quantiles.len());
                for q in quantiles.iter() {
                    if q.is_none() {
                        result.push(NoirType::NaN());
                    } else if q.as_ref().unwrap().count() == 0 {
                        result.push(NoirType::None());
                    } else {
                        let quantile = q.as_ref().unwrap().query(quantile).unwrap().1;
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
                        NoirData::NoirType(quantiles.query(quantile).unwrap().1)
                    }
                } else {
                    NoirData::NoirType(NoirType::NaN())
                }
            }
        })
    }

    pub fn gk(
        self,
        quantile: f64,
        error: f64,
        skip_na: bool,
    ) -> Stream<NoirData, impl Operator<NoirData>> {
        self.fold_assoc(
            (None, false),
            move |acc, v| {
                if !acc.1 {
                    acc.1 = v.gk(&mut acc.0, error, skip_na);
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
        .map(move |v| {
            let quantiles = v.0.unwrap();
            if quantiles.len() > 1 {
                let mut result = Vec::with_capacity(quantiles.len());
                for q in quantiles.iter() {
                    if q.is_none() {
                        result.push(NoirType::NaN());
                    } else if q.as_ref().unwrap().n() == 0 {
                        result.push(NoirType::None());
                    } else {
                        let quantile = q.as_ref().unwrap().quantile(quantile);
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
                        NoirData::NoirType(*(quantiles.quantile(quantile)))
                    }
                } else {
                    NoirData::NoirType(NoirType::NaN())
                }
            }
        })
    }

    pub fn p2(self, quantile: f64, skip_na: bool) -> Stream<NoirData, impl Operator<NoirData>> {
        self.flat_map(|item| match item {
            NoirData::Row(row) => {
                let mut res = Vec::with_capacity(row.len());
                for (i, el) in row.iter().enumerate() {
                    res.push((i, NoirData::NoirType(*el)));
                }
                res
            }
            NoirData::NoirType(_) => vec![(0, item)],
        })
        .group_by(|it| it.0)
        .fold((None, false), move |acc, v| {
            if !acc.1 {
                acc.1 = v.1.p2(&mut acc.0, quantile, skip_na);
            }
        })
        .map(|v| {
            if v.1 .1 {
                NoirData::NoirType(NoirType::NaN())
            } else if let Some(quantile) = v.1 .0 {
                if quantile.is_empty() {
                    NoirData::NoirType(NoirType::None())
                } else {
                    NoirData::NoirType(NoirType::from(quantile.quantile() as f32))
                }
            } else {
                NoirData::NoirType(NoirType::NaN())
            }
        })
        .unkey()
        .fold(Vec::new(), |acc, item| {
            if acc.len() <= item.0 {
                acc.resize(item.0 + 1, NoirType::None())
            }
            let v = acc.get_mut(item.0).unwrap();
            *v = item.1.to_type();
        })
        .map(|item| {
            if item.len() == 1 {
                NoirData::NoirType(item[0])
            } else {
                NoirData::Row(item)
            }
        })
    }
}
