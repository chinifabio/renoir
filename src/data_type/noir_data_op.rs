use std::fmt::Display;

use super::{NoirData, NoirType};

macro_rules! impl_comp{
    ($comp: tt, $v: ident, $n:ident) => {
        pub fn $n(self, $v: &mut Option<NoirData>, skip_na: bool) -> bool {
            // if we haven't found a NaN yet, update the $v.
            if $v.is_none() {
                // if the $v is None, initialize it.
                match &self {
                    NoirData::Row(row) => {
                        *$v = Some(NoirData::Row(vec![NoirType::None(); row.len()]));
                    },
                    NoirData::NoirType(_) => {
                        *$v = Some(NoirData::NoirType(NoirType::None()));
                    }
                }
            }

            match $v.as_mut().unwrap() {
                NoirData::Row(r) => {
                    let mut all_nan = true;
                    let row = self.to_row();
                    for (i, v) in row.into_iter().enumerate() {
                        // for each column, update the corrispondent $n.
                        if !r[i].is_nan() {
                            if !v.is_na(){
                                all_nan = false;
                                // item is not a NaN, check if it is smaller than the current $n.
                                if r[i].is_none() || v $comp r[i] {
                                    // if the item is smaller than the current $n, set the current $n to the item.
                                    r[i] = v;
                                }
                            } else {
                                // item is a NaN, check if we skip them.
                                if !skip_na {
                                    // if we don't skip them, set the current $n to NaN.
                                    r[i] = v;
                                } else {
                                    // if we skip them, keep the current $n as is.
                                    all_nan = false
                                }
                            }
                        }
                    }
                    return all_nan;
                },
                NoirData::NoirType($n) => {
                    let item = self.to_type();
                    // update the $v.
                    if !item.is_na() {
                        // item is not a NaN, check if it is smaller than the current $n.
                        if $n.is_none() || &item $comp &$n {
                            // if the item is smaller than the current $n, set the current $n to the item.
                            *$v = Some(NoirData::NoirType(item));
                        }
                        return false;
                    } else if !skip_na {
                        // if we don't skip them, set the current $n to NaN.
                        *$v = Some(NoirData::NoirType(item));
                        return true;
                    }
                    return false;
                }
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

    impl_comp!(<, min_item, min);
    impl_comp!(>, max_item, max);
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
