use std::fmt::Display;

use super::{NoirData, NoirType};

macro_rules! initialize {
    (&$s: ident , $v: ident) => {
        if $v.is_none() {
            match &$s {
                NoirData::Row(row) => {
                    *$v = Some(NoirData::Row(vec![NoirType::None(); row.len()]));
                },
                NoirData::NoirType(_) => {
                    *$v = Some(NoirData::NoirType(NoirType::None()));
                }
            }
        }
    };
}

macro_rules! impl_func{
    ($s:ident, $func_row: expr, $func_type: expr, $v: ident, $n:ident, $skip_na: ident) => {
        match $v.as_mut().unwrap() {
            NoirData::Row(r) => {
                let mut all_nan = true;
                let row = $s.to_row();
                for (i, v) in row.into_iter().enumerate() {
                    if !r[i].is_nan() {
                        if !v.is_na(){
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
            },
            NoirData::NoirType($n) => {
                let item = $s.to_type();
                if !item.is_na() {
                    $func_type($n ,item);
                    return false;
                } else if !$skip_na {
                    *$v = Some(NoirData::NoirType(item));
                    return true;
                }
                return false;
            }
        }
    }   
}

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

    pub fn to_row(self) -> Vec<NoirType> {
        match self {
            NoirData::Row(row) => row,
            NoirData::NoirType(_) => panic!("Cannot convert a type to a row"),
        }
    }

    pub fn get_row(&mut self) -> &mut Vec<NoirType>{
        match self {
            NoirData::Row(row) => row,
            NoirData::NoirType(_) => panic!("Cannot convert a type to a row"),
        }
    }

    pub fn get_type(&mut self) -> &mut NoirType{
        match self {
            NoirData::Row(_) => panic!("Cannot convert a row to a type"),
            NoirData::NoirType(v) => v,
        }
    }

    pub fn chen(count: &mut Option<NoirData>, mean: &mut Option<NoirData> ,m2: &mut Option<NoirData>, skip_na: bool, item: (NoirData, NoirData, NoirData)) -> bool {
       
        let r_mean = item.1;

        initialize!(&r_mean , mean);
        initialize!(&r_mean, m2);
        initialize!(&r_mean, count);

        impl_func!(r_mean, |i: usize, r: &mut Vec<NoirType>, v: NoirType| {

            let r_count = item.0.clone().to_row();
            let r_m2 = item.2.clone().to_row();

            let count_row = count.as_mut().unwrap().get_row();
            let m2_row: &mut Vec<NoirType>  = m2.as_mut().unwrap().get_row();

            if (*m2_row)[i].is_none() {
                count_row[i] = r_count[i];
                r[i] = v;
                (*m2_row)[i] = r_m2[i];
            }else{
                let old_count = count_row[i];
                count_row[i] += r_count[i];
                let delta = v - r[i];
                r[i] += delta * r_count[i] / count_row[i];
                (*m2_row)[i] += r_m2[i] + delta * ((old_count * r_count[i])/count_row[i]);
            };
        }, |avg: &mut NoirType, current: NoirType| {
                
            let r_count = item.0.to_type();
            let r_m2 = item.2.to_type();

            let count_item = count.as_mut().unwrap().get_type();
            let m2_item = m2.as_mut().unwrap().get_type();

            if (*m2_item).is_none() {
                *count_item = r_count;
                *avg = current;
                *m2_item = r_m2;
            }else{
                let old_count = *count_item;
                *count_item += r_count;
                let delta = current - *avg;
                *avg += delta * r_count / *count_item;
                *m2_item += r_m2 + delta * ((old_count * r_count)/ *count_item);
            }
            return false;
        }, mean, m, skip_na)
    }

    pub fn welford(self, count: &mut Option<NoirData>, mean: &mut Option<NoirData> ,m2: &mut Option<NoirData>, skip_na: bool) -> bool {

        initialize!(&self, mean);
        initialize!(&self, m2);
        initialize!(&self, count);

        impl_func!(self, |i: usize, r: &mut Vec<NoirType>, v: NoirType| {
            let count_row = count.as_mut().unwrap().get_row();
            let m2_row: &mut Vec<NoirType>  = m2.as_mut().unwrap().get_row();

            if count_row[i].is_none() {
                count_row[i] = NoirType::Int32(1);
            }else {
                count_row[i] += NoirType::Int32(1);
            }
            let new_mean = if r[i].is_none() {
                v
            } else {
                r[i] + (v - r[i]) / count_row[i]
            };
            if (*m2_row)[i].is_none() {
                (*m2_row)[i] = NoirType::Int32(0);
            }else{
                (*m2_row)[i] += (v - r[i]) * (v - new_mean);
            };
            r[i] = new_mean;
        }, |avg: &mut NoirType, item: NoirType| {

            let count_item = count.as_mut().unwrap().get_type();
            let m2_item = m2.as_mut().unwrap().get_type();

            if count_item.is_none() {
                *count_item = NoirType::Int32(1);
            }else {
                *count_item += NoirType::Int32(1);
            }
            let new_mean = if avg.is_none() {item} else {*avg + (item - *avg) / *count_item};
            if m2_item.is_none(){
                *m2_item = (item - *avg) * (item - new_mean);
            }else{
                *m2_item += (item - *avg) * (item - new_mean);
            }
            return false;
        }, mean, m, skip_na)
    }


    pub fn min(self, min_item: &mut Option<NoirData>, skip_na: bool) -> bool {
        initialize!(&self, min_item);
        impl_func!(self, |i: usize, r: &mut Vec<NoirType>, v: NoirType| {
            if r[i].is_none() || v < r[i] {
                r[i] = v;
            }
        } , |c_min: &mut NoirType, item: NoirType| {
            if c_min.is_none() || &item < &c_min {
                *c_min = item;
            }
        }, min_item, min, skip_na)
    }

    pub fn max(self, max_item: &mut Option<NoirData>, skip_na: bool) -> bool {
        initialize!(&self, max_item);
        impl_func!(self, |i: usize, r: &mut Vec<NoirType>, v: NoirType| {
            if r[i].is_none() || v > r[i] {
                r[i] = v;
            }
        } , |c_max: &mut NoirType, item: NoirType| {
            if c_max.is_none() || &item > &c_max {
                *c_max = item;
            }
        }, max_item, max, skip_na)
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
        match (self, other) {
            (NoirData::Row(a), NoirData::Row(b)) => a.partial_cmp(b),
            (NoirData::NoirType(a), NoirData::NoirType(b)) => a.partial_cmp(b),
            (_, _) => panic!("Type mismatch!"),
        }
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