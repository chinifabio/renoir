use std::{
    collections::HashMap,
    fmt::Display,
    ops::{Index, IndexMut},
    usize,
};

use crate::{data_type::noir_type::NoirType, optimization::dsl::expressions::ExprEvaluable};
use average::{Estimate, Quantile};
use quantiles::ckms::CKMS;
use serde::{Deserialize, Serialize};
use sha2::digest::typenum::Pow;

use super::{greenwald_khanna::Gka, stream_item::StreamItem};

macro_rules! initialize {
    (&$s: ident , $v: ident) => {
        if $v.is_none() {
            match &$s {
                NoirData::Row(row) => {
                    *$v = Some(NoirData::Row(vec![NoirType::None(); row.len()]));
                }
                NoirData::NoirType(_) => {
                    *$v = Some(NoirData::NoirType(NoirType::None()));
                }
            }
        }
    };
}

macro_rules! impl_func {
    ($self:ident, $func_row: expr, $func_type: expr, $change: ident, $change_type:ident, $skip_na: ident) => {
        match $change.as_mut().unwrap() {
            NoirData::Row(r) => {
                let mut all_nan = true;
                let row = $self.to_row();
                for (i, v) in row.into_iter().enumerate() {
                    if !r[i].is_nan() {
                        if !v.is_na() {
                            all_nan = false;
                            $func_row(i, r, v);
                        } else {
                            if !$skip_na {
                                r[i] = v;
                            } else {
                                all_nan = false;
                            }
                        }
                    }
                }
                return all_nan;
            }
            NoirData::NoirType($change_type) => {
                let item = $self.to_type();
                if !item.is_na() {
                    $func_type($change_type, item);
                    return false;
                } else if !$skip_na {
                    *$change = Some(NoirData::NoirType(item));
                    return true;
                }
                return false;
            }
        }
    };
}

/// NoirData is the data type that is used in Noir.
/// It can be either a row of NoirType or a single NoirType, this reduce the allocation of memory necessary
/// when we are dealing with a single value.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Hash)]
pub enum NoirData {
    Row(Vec<NoirType>),
    NoirType(NoirType),
}

#[allow(clippy::redundant_closure_call)]
#[allow(clippy::assign_op_pattern)]
impl NoirData {
    pub fn new(columns: Vec<NoirType>) -> NoirData {
        if columns.len() == 1 {
            NoirData::NoirType(columns[0])
        } else {
            NoirData::Row(columns)
        }
    }

    pub fn columns(self) -> Option<Vec<NoirType>> {
        match self {
            NoirData::Row(row) => Some(row),
            NoirData::NoirType(_) => None,
        }
    }

    pub fn new_empty() -> NoirData {
        NoirData::Row(vec![])
    }

    pub fn len(&self) -> usize {
        match self {
            NoirData::Row(row) => row.len(),
            NoirData::NoirType(_) => 1,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            NoirData::Row(row) => row.is_empty(),
            NoirData::NoirType(_) => false,
        }
    }

    pub fn contains_none(&self) -> bool {
        match self {
            NoirData::Row(row) => row.iter().any(|item| item.is_none()),
            NoirData::NoirType(v) => v.is_none(),
        }
    }

    pub fn to_type(self) -> NoirType {
        match self {
            NoirData::Row(_) => panic!("Cannot convert a row to a type"),
            NoirData::NoirType(v) => v,
        }
    }

    pub fn get_type(&mut self) -> &mut NoirType {
        match self {
            NoirData::Row(_) => panic!("Cannot convert a row to a type"),
            NoirData::NoirType(v) => v,
        }
    }

    pub fn type_(&self) -> &NoirType {
        match self {
            NoirData::Row(_) => panic!("Cannot convert a row to a type"),
            NoirData::NoirType(v) => v,
        }
    }

    pub fn to_row(self) -> Vec<NoirType> {
        match self {
            NoirData::Row(row) => row,
            NoirData::NoirType(_) => panic!("Cannot convert a type to a row"),
        }
    }

    pub fn get_row(&mut self) -> &mut Vec<NoirType> {
        match self {
            NoirData::Row(row) => row,
            NoirData::NoirType(_) => panic!("Cannot convert a type to a row"),
        }
    }

    pub fn row(&self) -> &Vec<NoirType> {
        match self {
            NoirData::Row(row) => row,
            NoirData::NoirType(_) => panic!("Cannot convert a type to a row"),
        }
    }

    pub fn or(self, other: &NoirData) -> NoirData {
        match (self, other) {
            (NoirData::Row(a), NoirData::Row(b)) => {
                NoirData::Row(a.into_iter().zip(b.iter()).map(|(a, b)| a.or(b)).collect())
            }
            (NoirData::NoirType(a), NoirData::NoirType(b)) => NoirData::NoirType(a.or(b)),
            (_, _) => panic!("Type mismatch!"),
        }
    }

    pub fn skew_kurt(
        self,
        skew: &mut Option<NoirData>,
        count: &NoirData,
        mean: &NoirData,
        std: &NoirData,
        skip_na: bool,
        exp: i32,
    ) -> bool {
        initialize!(&self, skew);

        impl_func!(
            self,
            |i: usize, skew: &mut Vec<NoirType>, item: NoirType| {
                let mean_row = mean.row();
                let count_row = count.row();
                let std_row = std.row();

                if mean_row[i].is_na() {
                    skew[i] = mean_row[i];
                } else {
                    if skew[i].is_none() {
                        skew[i] = NoirType::Float32(0.0);
                    }
                    if std_row[i] != NoirType::Float32(0.0) {
                        skew[i] = skew[i]
                            + ((item - mean_row[i]).powi(exp))
                                / (count_row[i] * std_row[i].powi(exp));
                    }
                }
            },
            |skew: &mut NoirType, item: NoirType| {
                let mean_item = mean.type_();
                let count_item = count.type_();
                let std_item = std.type_();

                if mean_item.is_na() {
                    *skew = *mean_item;
                } else {
                    if skew.is_none() {
                        *skew = NoirType::Float32(0.0);
                    }
                    if *std_item != NoirType::Float32(0.0) {
                        *skew = *skew
                            + ((item - mean_item).powi(exp)) / (*count_item * std_item.powi(exp));
                    }
                }
            },
            skew,
            s,
            skip_na
        )
    }

    pub fn chen(
        count: &mut Option<NoirData>,
        mean: &mut Option<NoirData>,
        m2: &mut Option<NoirData>,
        skip_na: bool,
        item: (NoirData, NoirData, NoirData),
    ) -> bool {
        let r_mean = item.1;

        initialize!(&r_mean, mean);
        initialize!(&r_mean, m2);
        initialize!(&r_mean, count);

        impl_func!(
            r_mean,
            |i: usize, r: &mut Vec<NoirType>, v: NoirType| {
                let r_count = item.0.clone().to_row();
                let r_m2 = item.2.clone().to_row();

                let count_row = count.as_mut().unwrap().get_row();
                let m2_row: &mut Vec<NoirType> = m2.as_mut().unwrap().get_row();

                if (*m2_row)[i].is_none() {
                    count_row[i] = r_count[i];
                    r[i] = v;
                    (*m2_row)[i] = r_m2[i];
                } else {
                    let old_count = count_row[i];
                    count_row[i] += r_count[i];
                    let delta = v - r[i];
                    r[i] = r[i] + delta * r_count[i] / count_row[i];
                    (*m2_row)[i] = (*m2_row)[i]
                        + r_m2[i]
                        + delta.powi(2) * ((old_count * r_count[i]) / count_row[i]);
                };
            },
            |avg: &mut NoirType, current: NoirType| {
                let r_count = item.0.to_type();
                let r_m2 = item.2.to_type();

                let count_item = count.as_mut().unwrap().get_type();
                let m2_item = m2.as_mut().unwrap().get_type();

                if (*m2_item).is_none() {
                    *count_item = r_count;
                    *avg = current;
                    *m2_item = r_m2;
                } else {
                    let old_count = *count_item;
                    *count_item += r_count;
                    let delta = current - *avg;
                    *avg = *avg + delta * r_count / *count_item;
                    *m2_item =
                        *m2_item + r_m2 + delta.powi(2) * ((old_count * r_count) / *count_item);
                }

                false
            },
            mean,
            m,
            skip_na
        )
    }

    pub fn welford(
        self,
        count: &mut Option<NoirData>,
        mean: &mut Option<NoirData>,
        m2: &mut Option<NoirData>,
        skip_na: bool,
    ) -> bool {
        initialize!(&self, mean);
        initialize!(&self, m2);
        initialize!(&self, count);

        impl_func!(
            self,
            |i: usize, r: &mut Vec<NoirType>, v: NoirType| {
                let count_row = count.as_mut().unwrap().get_row();
                let m2_row: &mut Vec<NoirType> = m2.as_mut().unwrap().get_row();

                if count_row[i].is_none() {
                    count_row[i] = NoirType::Int32(1);
                } else {
                    count_row[i] += NoirType::Int32(1);
                }
                let new_mean = if r[i].is_none() {
                    v
                } else {
                    r[i] + ((v - r[i]) / count_row[i])
                };
                if (*m2_row)[i].is_none() {
                    (*m2_row)[i] = NoirType::Int32(0);
                } else {
                    (*m2_row)[i] = (*m2_row)[i] + (v - r[i]) * (v - new_mean);
                };
                r[i] = new_mean;
            },
            |avg: &mut NoirType, item: NoirType| {
                let count_item = count.as_mut().unwrap().get_type();
                let m2_item = m2.as_mut().unwrap().get_type();

                if count_item.is_none() {
                    *count_item = NoirType::Int32(1);
                } else {
                    *count_item += NoirType::Int32(1);
                }
                let new_mean = if avg.is_none() {
                    item
                } else {
                    *avg + ((item - *avg) / *count_item)
                };
                if m2_item.is_none() {
                    *m2_item = NoirType::Int32(0);
                } else {
                    *m2_item = *m2_item + (item - *avg) * (item - new_mean);
                }
                *avg = new_mean;
            },
            mean,
            m,
            skip_na
        )
    }

    pub fn mean(sum: &mut Option<NoirData>, count: NoirData, skip_na: bool) -> bool {
        impl_func!(
            count,
            |i: usize, r: &mut Vec<NoirType>, v: NoirType| {
                r[i] = r[i] / v;
            },
            |avg: &mut NoirType, c: NoirType| {
                *avg = *avg / c;
            },
            sum,
            m,
            skip_na
        );
    }

    pub fn global_sum_count(
        sum: &mut Option<NoirData>,
        count: &mut Option<NoirData>,
        skip_na: bool,
        item: (NoirData, NoirData),
    ) -> bool {
        let r_sum = item.0;

        initialize!(&r_sum, sum);
        initialize!(&r_sum, count);

        impl_func!(
            r_sum,
            |i: usize, r: &mut Vec<NoirType>, v: NoirType| {
                let count_row = count.as_mut().unwrap().get_row();
                let remote_count = item.1.row();

                if count_row[i].is_none() {
                    count_row[i] = remote_count[i];
                } else {
                    count_row[i] = count_row[i] + remote_count[i];
                }

                if r[i].is_none() {
                    r[i] = v;
                } else {
                    r[i] = r[i] + v;
                }
            },
            |c_sum: &mut NoirType, it: NoirType| {
                let count_item = count.as_mut().unwrap().get_type();
                let remote_count = item.1.to_type();

                if count_item.is_none() {
                    *count_item = remote_count;
                } else {
                    *count_item = *count_item + remote_count;
                }

                if c_sum.is_none() {
                    *c_sum = it;
                } else {
                    *c_sum = *c_sum + it;
                }
            },
            sum,
            s,
            skip_na
        );
    }

    pub fn sum(self, sum: &mut Option<NoirData>, skip_na: bool) -> bool {
        initialize!(&self, sum);
        impl_func!(
            self,
            |i: usize, r: &mut Vec<NoirType>, v: NoirType| {
                if r[i].is_none() {
                    r[i] = v;
                } else {
                    r[i] = r[i] + v;
                }
            },
            |c_sum: &mut NoirType, item: NoirType| {
                if c_sum.is_none() {
                    *c_sum = item;
                } else {
                    *c_sum = *c_sum + item;
                }
            },
            sum,
            s,
            skip_na
        )
    }

    pub fn sum_count(
        self,
        sum: &mut Option<NoirData>,
        count: &mut Option<NoirData>,
        skip_na: bool,
    ) -> bool {
        initialize!(&self, sum);
        initialize!(&self, count);
        impl_func!(
            self,
            |i: usize, r: &mut Vec<NoirType>, v: NoirType| {
                let count_row = count.as_mut().unwrap().get_row();
                if count_row[i].is_none() {
                    count_row[i] = NoirType::Int32(1);
                } else {
                    count_row[i] += NoirType::Int32(1);
                }

                if r[i].is_none() {
                    r[i] = v;
                } else {
                    r[i] = r[i] + v;
                }
            },
            |c_sum: &mut NoirType, item: NoirType| {
                let count_item = count.as_mut().unwrap().get_type();
                if count_item.is_none() {
                    *count_item = NoirType::Int32(1);
                } else {
                    *count_item += NoirType::Int32(1);
                }

                if c_sum.is_none() {
                    *c_sum = item;
                } else {
                    *c_sum = *c_sum + item;
                }
            },
            sum,
            s,
            skip_na
        )
    }

    pub fn global_count_kumulant_4(
        count: &mut Option<NoirData>,
        s1: &mut Option<NoirData>,
        s2: &mut Option<NoirData>,
        s3: &mut Option<NoirData>,
        s4: &mut Option<NoirData>,
        skip_na: bool,
        item: (NoirData, NoirData, NoirData, NoirData, NoirData),
    ) -> bool {
        let r_count = item.0;

        initialize!(&r_count, count);
        initialize!(&r_count, s1);
        initialize!(&r_count, s2);
        initialize!(&r_count, s3);
        initialize!(&r_count, s4);

        impl_func!(
            r_count,
            |i: usize, r: &mut Vec<NoirType>, v: NoirType| {
                let s1_item = s1.as_mut().unwrap().get_row();
                let s2_item = s2.as_mut().unwrap().get_row();
                let s3_item = s3.as_mut().unwrap().get_row();
                let s4_item = s4.as_mut().unwrap().get_row();

                let remote_s1 = item.1.row();
                let remote_s2 = item.2.row();
                let remote_s3 = item.3.row();
                let remote_s4 = item.4.row();
                if s1_item[i].is_none() {
                    s1_item[i] = remote_s1[i];
                } else {
                    s1_item[i] += remote_s1[i];
                }
                if s2_item[i].is_none() {
                    s2_item[i] = remote_s2[i];
                } else {
                    s2_item[i] += remote_s2[i];
                }
                if s3_item[i].is_none() {
                    s3_item[i] = remote_s3[i];
                } else {
                    s3_item[i] += remote_s3[i];
                }
                if s4_item[i].is_none() {
                    s4_item[i] = remote_s4[i];
                } else {
                    s4_item[i] += remote_s4[i];
                }
                if r[i].is_none() {
                    r[i] = v;
                } else {
                    r[i] = r[i] + v;
                }
            },
            |c_count: &mut NoirType, it: NoirType| {
                let s1_item = s1.as_mut().unwrap().get_type();
                let s2_item = s2.as_mut().unwrap().get_type();
                let s3_item = s3.as_mut().unwrap().get_type();
                let s4_item = s4.as_mut().unwrap().get_type();

                let remote_s1 = item.1.to_type();
                let remote_s2 = item.2.to_type();
                let remote_s3 = item.3.to_type();
                let remote_s4 = item.4.to_type();
                if s1_item.is_none() {
                    *s1_item = remote_s1;
                } else {
                    *s1_item += remote_s1;
                }
                if s2_item.is_none() {
                    *s2_item = remote_s2;
                } else {
                    *s2_item += remote_s2;
                }
                if s3_item.is_none() {
                    *s3_item = remote_s3;
                } else {
                    *s3_item += remote_s3;
                }
                if s4_item.is_none() {
                    *s4_item = remote_s4;
                } else {
                    *s4_item += remote_s4;
                }
                if c_count.is_none() {
                    *c_count = it;
                } else {
                    *c_count = *c_count + it;
                }
            },
            count,
            c,
            skip_na
        );
    }

    pub fn count_kumulant_4(
        self,
        count: &mut Option<NoirData>,
        s1: &mut Option<NoirData>,
        s2: &mut Option<NoirData>,
        s3: &mut Option<NoirData>,
        s4: &mut Option<NoirData>,
        skip_na: bool,
    ) -> bool {
        initialize!(&self, count);
        initialize!(&self, s1);
        initialize!(&self, s2);
        initialize!(&self, s3);
        initialize!(&self, s4);

        impl_func!(
            self,
            |i: usize, r: &mut Vec<NoirType>, v: NoirType| {
                let s1_item = s1.as_mut().unwrap().get_row();
                if s1_item[i].is_none() {
                    s1_item[i] = v;
                } else {
                    s1_item[i] += v;
                }

                let s2_item = s2.as_mut().unwrap().get_row();
                if s2_item[i].is_none() {
                    s2_item[i] = v.powi(2);
                } else {
                    s2_item[i] += v.powi(2);
                }

                let s3_item = s3.as_mut().unwrap().get_row();
                if s3_item[i].is_none() {
                    s3_item[i] = v.powi(3);
                } else {
                    s3_item[i] += v.powi(3);
                }

                let s4_item = s4.as_mut().unwrap().get_row();
                if s4_item[i].is_none() {
                    s4_item[i] = v.powi(4);
                } else {
                    s4_item[i] += v.powi(4);
                }

                if r[i].is_none() {
                    r[i] = NoirType::Int32(1);
                } else {
                    r[i] += NoirType::Int32(1);
                }
            },
            |c_count: &mut NoirType, item: NoirType| {
                let s1_item = s1.as_mut().unwrap().get_type();
                if s1_item.is_none() {
                    *s1_item = item;
                } else {
                    *s1_item += item;
                }

                let s2_item = s2.as_mut().unwrap().get_type();
                if s2_item.is_none() {
                    *s2_item = item.powi(2);
                } else {
                    *s2_item += item.powi(2);
                }

                let s3_item = s3.as_mut().unwrap().get_type();
                if s3_item.is_none() {
                    *s3_item = item.powi(3);
                } else {
                    *s3_item += item.powi(3);
                }

                let s4_item = s4.as_mut().unwrap().get_type();
                if s4_item.is_none() {
                    *s4_item = item.powi(4);
                } else {
                    *s4_item += item.powi(4);
                }

                if c_count.is_none() {
                    *c_count = NoirType::Int32(1);
                } else {
                    *c_count += NoirType::Int32(1);
                }
            },
            count,
            c,
            skip_na
        )
    }

    pub fn min(self, min_item: &mut Option<NoirData>, skip_na: bool) -> bool {
        initialize!(&self, min_item);
        impl_func!(
            self,
            |i: usize, r: &mut Vec<NoirType>, v: NoirType| {
                if r[i].is_none() || v < r[i] {
                    r[i] = v;
                }
            },
            |c_min: &mut NoirType, item: NoirType| {
                if c_min.is_none() || &item < c_min {
                    *c_min = item;
                }
            },
            min_item,
            min,
            skip_na
        )
    }

    pub fn max(self, max_item: &mut Option<NoirData>, skip_na: bool) -> bool {
        initialize!(&self, max_item);
        impl_func!(
            self,
            |i: usize, r: &mut Vec<NoirType>, v: NoirType| {
                if r[i].is_none() || v > r[i] {
                    r[i] = v;
                }
            },
            |c_max: &mut NoirType, item: NoirType| {
                if c_max.is_none() || &item > c_max {
                    *c_max = item;
                }
            },
            max_item,
            max,
            skip_na
        )
    }

    pub fn p2(self, quantiles: &mut Option<Quantile>, q: f64, skip_na: bool) -> bool {
        if quantiles.is_none() {
            *quantiles = Some(Quantile::new(q));
        }

        if let Some(quant) = quantiles {
            let item = self.to_type();
            if !item.is_na() {
                quant.add(item.into());
                return false;
            } else if !skip_na {
                return true;
            }
            return false;
        }
        unreachable!("Quantiles should be initialized!");
    }

    pub fn gk(
        self,
        quantiles: &mut Option<Vec<Option<Gka<NoirType>>>>,
        error: f64,
        skip_na: bool,
    ) -> bool {
        if quantiles.is_none() {
            match &self {
                NoirData::Row(row) => {
                    *quantiles = Some(vec![Some(Gka::new(error)); row.len()]);
                }
                NoirData::NoirType(_) => {
                    *quantiles = Some(vec![Some(Gka::new(error))]);
                }
            }
        }

        if let Some(quant) = quantiles {
            if quant.len() > 1 {
                let mut all_nan = true;
                let row = self.to_row();
                for (i, v) in row.into_iter().enumerate() {
                    if quant[i].is_some() {
                        if !v.is_na() {
                            all_nan = false;
                            quant[i].as_mut().unwrap().insert(v);
                        } else if !skip_na {
                            quant[i] = None;
                        } else {
                            all_nan = false;
                        }
                    }
                }
                return all_nan;
            } else {
                let item = self.to_type();
                if !item.is_na() {
                    quant[0].as_mut().unwrap().insert(item);
                    return false;
                } else if !skip_na {
                    quant[0] = None;
                    return true;
                }
                return false;
            }
        }
        unreachable!("Quantiles should be initialized!");
    }

    pub fn ckms(
        self,
        quantiles: &mut Option<Vec<Option<CKMS<NoirType>>>>,
        error: f64,
        skip_na: bool,
    ) -> bool {
        if quantiles.is_none() {
            match &self {
                NoirData::Row(row) => {
                    *quantiles = Some(vec![Some(CKMS::new(error)); row.len()]);
                }
                NoirData::NoirType(_) => {
                    *quantiles = Some(vec![Some(CKMS::new(error))]);
                }
            }
        }

        if let Some(quant) = quantiles {
            if quant.len() > 1 {
                let mut all_nan = true;
                let row = self.to_row();
                for (i, v) in row.into_iter().enumerate() {
                    if quant[i].is_some() {
                        if !v.is_na() {
                            all_nan = false;
                            quant[i].as_mut().unwrap().insert(v);
                        } else if !skip_na {
                            quant[i] = None;
                        } else {
                            all_nan = false;
                        }
                    }
                }
                return all_nan;
            } else {
                let item = self.to_type();
                if !item.is_na() {
                    quant[0].as_mut().unwrap().insert(item);
                    return false;
                } else if !skip_na {
                    quant[0] = None;
                    return true;
                }
                return false;
            }
        }
        unreachable!("Quantiles should be initialized!");
    }

    pub fn mode_count(
        self,
        bins: &mut Option<Vec<Option<HashMap<i32, usize>>>>,
        counts: &mut Option<Vec<usize>>,
        skip_na: bool,
    ) -> bool {
        match self {
            NoirData::Row(row) => {
                if bins.is_none() {
                    *bins = Some(vec![Some(HashMap::new()); row.len()]);
                    *counts = Some(vec![0; row.len()]);
                }
                let mut all_nan = true;
                for (i, r) in row.iter().enumerate() {
                    if !r.is_na() {
                        all_nan = false;
                        let bin = bins.as_mut().unwrap();
                        match r {
                            NoirType::Int32(k) => {
                                if bin[i].is_some() {
                                    let count = bin[i].as_mut().unwrap().entry(*k).or_insert(0);
                                    *count += 1;
                                    counts.as_mut().unwrap()[i] += 1;
                                }
                            }
                            NoirType::Float32(_) => panic!("Mode supported only for int!"),
                            _ => panic!("NaN or None!"),
                        }
                    } else if !skip_na {
                        let bin = bins.as_mut().unwrap();
                        bin[i] = None;
                    } else {
                        all_nan = false;
                    }
                }

                all_nan
            }
            NoirData::NoirType(value) => {
                if bins.is_none() {
                    *bins = Some(vec![Some(HashMap::new())]);
                    *counts = Some(vec![0]);
                }
                let bin = bins.as_mut().unwrap();
                if !value.is_na() {
                    match value {
                        NoirType::Int32(k) => {
                            let count = bin[0].as_mut().unwrap().entry(k).or_insert(0);
                            *count += 1;
                            counts.as_mut().unwrap()[0] += 1;
                        }
                        NoirType::Float32(_) => panic!("Mode supported only for int!"),
                        _ => panic!("NaN or None!"),
                    }
                    false
                } else if !skip_na {
                    bin[0] = None;
                    return true;
                } else {
                    return false;
                }
            }
        }
    }

    pub fn mode(self, bins: &mut Option<Vec<Option<HashMap<i32, usize>>>>, skip_na: bool) -> bool {
        match self {
            NoirData::Row(row) => {
                if bins.is_none() {
                    *bins = Some(vec![Some(HashMap::new()); row.len()]);
                }
                let mut all_nan = true;
                for (i, r) in row.iter().enumerate() {
                    if !r.is_na() {
                        all_nan = false;
                        let bin = bins.as_mut().unwrap();
                        match r {
                            NoirType::Int32(k) => {
                                if bin[i].is_some() {
                                    let count = bin[i].as_mut().unwrap().entry(*k).or_insert(0);
                                    *count += 1;
                                }
                            }
                            NoirType::Float32(_) => panic!("Mode supported only for int!"),
                            _ => panic!("NaN or None!"),
                        }
                    } else if !skip_na {
                        let bin = bins.as_mut().unwrap();
                        bin[i] = None;
                    } else {
                        all_nan = false;
                    }
                }

                all_nan
            }
            NoirData::NoirType(value) => {
                if bins.is_none() {
                    *bins = Some(vec![Some(HashMap::new())]);
                }
                let bin = bins.as_mut().unwrap();
                if !value.is_na() {
                    match value {
                        NoirType::Int32(k) => {
                            let count = bin[0].as_mut().unwrap().entry(k).or_insert(0);
                            *count += 1;
                        }
                        NoirType::Float32(_) => panic!("Mode supported only for int!"),
                        _ => panic!("NaN or None!"),
                    }
                    false
                } else if !skip_na {
                    bin[0] = None;
                    return true;
                } else {
                    return false;
                }
            }
        }
    }

    pub(crate) fn covariance(&self, cov: &mut Option<NoirData>, count: &NoirData, mean: &NoirData) {
        if cov.is_none() {
            *cov = Some(NoirData::NoirType(NoirType::None()))
        }

        let cov = cov.as_mut().unwrap().get_type();

        let data = self.row();
        let means = mean.row();
        let count = count.type_();

        if cov.is_none() {
            *cov = ((data[0] - means[0]) * (data[1] - means[1])) / count;
        } else {
            *cov += ((data[0] - means[0]) * (data[1] - means[1])) / count;
        }
    }

    pub fn pearson(
        &self,
        corr: &mut Option<NoirData>,
        count: &NoirData,
        mean: &NoirData,
        std: &NoirData,
    ) {
        if corr.is_none() {
            *corr = Some(NoirData::NoirType(NoirType::None()))
        }

        let cov = corr.as_mut().unwrap().get_type();

        let data = self.row();
        let means = mean.row();
        let count = count.type_();
        let stds = std.row();

        if cov.is_none() {
            *cov = ((data[0] - means[0]) * (data[1] - means[1])) / (*count * stds[0] * stds[1]);
        } else {
            *cov += ((data[0] - means[0]) * (data[1] - means[1])) / (*count * stds[0] * stds[1]);
        }
    }
}

impl Display for NoirData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NoirData::Row(row) => {
                write!(f, "[")?;
                for (i, item) in row.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            NoirData::NoirType(t) => write!(f, "{}", t),
        }
    }
}

impl PartialOrd for NoirData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NoirData {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (NoirData::Row(a), NoirData::Row(b)) => a.cmp(b),
            (NoirData::NoirType(a), NoirData::NoirType(b)) => a.cmp(b),
            (_, _) => panic!("Type mismatch!"),
        }
    }
}

impl Eq for NoirData {}

impl From<NoirDataCsv> for NoirData {
    fn from(value: NoirDataCsv) -> Self {
        match value {
            NoirDataCsv::Row(row) => NoirData::Row(row),
            NoirDataCsv::NoirType(v) => NoirData::NoirType(v),
        }
    }
}

impl From<StreamItem> for NoirData {
    fn from(value: StreamItem) -> Self {
        NoirData::Row(Vec::from(value))
    }
}

impl Index<usize> for NoirData {
    type Output = NoirType;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            NoirData::Row(row) => &row[index],
            NoirData::NoirType(a) => {
                if index == 0 {
                    a
                } else {
                    panic!(
                        "NOIR DATA: Index {} out of bounds for single col [{}]!",
                        index, self
                    )
                }
            }
        }
    }
}

impl IndexMut<usize> for NoirData {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        match self {
            NoirData::Row(row) => &mut row[index],
            NoirData::NoirType(a) => {
                if index == 0 {
                    a
                } else {
                    panic!("NOIR DATA: Index {} out of bounds for single col!", index)
                }
            }
        }
    }
}

impl ExprEvaluable for NoirData {
    fn as_ptr(&self) -> *const NoirType {
        match self {
            NoirData::Row(row) => row.as_ptr(),
            NoirData::NoirType(v) => v as *const NoirType,
        }
    }
}

#[derive(Clone, Debug)]
pub enum NoirDataCsv {
    Row(Vec<NoirType>),
    NoirType(NoirType),
}
