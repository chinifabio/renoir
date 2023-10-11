use std::fmt::Display;

use super::{NoirData, NoirType};

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

    pub fn contains_none(&self) ->bool {
        match self {
            NoirData::Row(row) => row.iter().any(|item| item.is_none()),
            NoirData::NoirType(v) => v.is_none(),
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
