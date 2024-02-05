use std::{
    fmt::Display,
    ops::{Index, IndexMut},
    path::PathBuf,
};

use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};

use crate::stream::KeyedItem;

mod greenwald_khanna;
mod noir_data_op;
mod noir_deserialize;
mod noir_type_op;

/// NoirType is the basic data type in Noir.
/// It can be either an Int32 or a Float32.
/// NaN defines a value that cannot be used in any calculation and the operators should be able to handle it.
/// None defines a missing value.
#[derive(Clone, Deserialize, Serialize, Debug, Copy)]
pub enum NoirType {
    Int32(i32),
    Float32(f32),
    Bool(bool),
    NaN(),
    None(),
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq, Hash, Copy)]
pub enum NoirTypeKind {
    Int32,
    Float32,
    Bool,
    NaN,
    None,
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq, Hash)]
pub struct Schema {
    pub(crate) columns: Vec<NoirTypeKind>,
}

/// NoirData is the data type that is used in Noir.
/// It can be either a row of NoirType or a single NoirType, this reduce the allocation of memory necessary
/// when we are dealing with a single value.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Hash)]
pub enum NoirData {
    Row(Vec<NoirType>),
    NoirType(NoirType),
}

#[derive(Clone, Debug)]
pub enum NoirDataCsv {
    Row(Vec<NoirType>),
    NoirType(NoirType),
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum StreamItem {
    DataItem(NoirData),
    JoinItem(NoirData, NoirData),
    KeyedDataItem(Vec<NoirType>, NoirData),
    KeyedJoinItem(Vec<NoirType>, NoirData, NoirData),
}

impl StreamItem {
    pub(crate) fn absorb_key(self, k: Vec<NoirType>) -> StreamItem {
        match self {
            StreamItem::DataItem(d) => StreamItem::KeyedDataItem(k, d),
            StreamItem::JoinItem(d1, d2) => StreamItem::KeyedJoinItem(k, d1, d2),
            _ => panic!("Item already has a key"),
        }
    }

    pub(crate) fn drop_key(self) -> StreamItem {
        match self {
            StreamItem::KeyedDataItem(_, v) => StreamItem::DataItem(v),
            StreamItem::KeyedJoinItem(_, v1, v2) => StreamItem::JoinItem(v1, v2),
            _ => panic!("Item has no key to drop"),
        }
    }

    pub(crate) fn get_key(&self) -> Option<Vec<NoirType>> {
        match self {
            StreamItem::KeyedDataItem(k, _) => Some(k.clone()),
            StreamItem::KeyedJoinItem(k, _, _) => Some(k.clone()),
            _ => None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            StreamItem::DataItem(d) => d.len(),
            StreamItem::JoinItem(d1, d2) => d1.len() + d2.len(),
            StreamItem::KeyedDataItem(_, d) => d.len(),
            StreamItem::KeyedJoinItem(_, d1, d2) => d1.len() + d2.len(),
        }
    }
}

impl KeyedItem for StreamItem {
    type Key = Vec<NoirType>;

    type Value = NoirData;

    fn key(&self) -> &Self::Key {
        match self {
            StreamItem::KeyedDataItem(k, _) => k,
            StreamItem::KeyedJoinItem(k, _, _) => k,
            _ => panic!("StreamItem has no key"),
        }
    }

    fn value(&self) -> &Self::Value {
        match self {
            StreamItem::DataItem(v) => v,
            StreamItem::JoinItem(_v1, _v2) => {
                // let mut data = Vec::with_capacity(v1.len() + v2.len());
                // data.extend(v1.row());
                // data.extend(v2.row());
                // &NoirData::Row(data)
                panic!("For now, i can't return a reference to a joined data")
            }
            StreamItem::KeyedDataItem(_, v) => v,
            StreamItem::KeyedJoinItem(_, _v1, _v2) => {
                // let mut data = Vec::with_capacity(v1.len() + v2.len());
                // data.extend(v1.row());
                // data.extend(v2.row());
                // &NoirData::Row(data)
                panic!("For now, i can't return a reference to a joined data")
            }
        }
    }

    fn into_kv(self) -> (Self::Key, Self::Value) {
        match self {
            StreamItem::KeyedDataItem(k, v) => (k, v),
            StreamItem::KeyedJoinItem(k, v1, v2) => {
                let mut data = Vec::with_capacity(v1.len() + v2.len());
                data.extend(v1.row());
                data.extend(v2.row());
                (k, NoirData::Row(data))
            }
            _ => panic!("StreamItem has no key"),
        }
    }
}

impl From<StreamItem> for NoirData {
    fn from(item: StreamItem) -> Self {
        match item {
            StreamItem::DataItem(d) => d,
            StreamItem::JoinItem(d1, d2) => {
                let mut data = Vec::with_capacity(d1.len() + d2.len());
                data.extend(d1.row());
                data.extend(d2.row());
                NoirData::Row(data)
            }
            StreamItem::KeyedDataItem(_, d) => d,
            StreamItem::KeyedJoinItem(_, d1, d2) => {
                let mut data = Vec::with_capacity(d1.len() + d2.len());
                data.extend(d1.row());
                data.extend(d2.row());
                NoirData::Row(data)
            }
        }
    }
}

impl From<NoirData> for StreamItem {
    fn from(data: NoirData) -> Self {
        StreamItem::DataItem(data)
    }
}

impl From<Vec<NoirType>> for StreamItem {
    fn from(data: Vec<NoirType>) -> Self {
        if data.len() == 1 {
            StreamItem::DataItem(NoirData::NoirType(data[0]))
        } else {
            StreamItem::DataItem(NoirData::Row(data))
        }
    }
}

impl From<(Vec<NoirType>, (StreamItem, StreamItem))> for StreamItem {
    fn from(data: (Vec<NoirType>, (StreamItem, StreamItem))) -> Self {
        StreamItem::KeyedJoinItem(data.0, data.1 .0.into(), data.1 .1.into())
    }
}

impl From<(Vec<NoirType>, NoirData)> for StreamItem {
    fn from(data: (Vec<NoirType>, NoirData)) -> Self {
        StreamItem::KeyedDataItem(data.0, data.1)
    }
}

impl Index<usize> for StreamItem {
    type Output = NoirType;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            StreamItem::DataItem(d) => &d[index],
            StreamItem::JoinItem(d1, d2) => {
                if index < d1.len() {
                    &d1[index]
                } else {
                    &d2[index - d1.len()]
                }
            }
            StreamItem::KeyedDataItem(_, d) => &d[index],
            StreamItem::KeyedJoinItem(_, d1, d2) => {
                if index < d1.len() {
                    &d1[index]
                } else {
                    &d2[index - d1.len()]
                }
            }
        }
    }
}

impl IndexMut<usize> for StreamItem {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        match self {
            StreamItem::DataItem(d) => &mut d[index],
            StreamItem::JoinItem(d1, d2) => {
                if index < d1.len() {
                    &mut d1[index]
                } else {
                    &mut d2[index - d1.len()]
                }
            }
            StreamItem::KeyedDataItem(_, d) => &mut d[index],
            StreamItem::KeyedJoinItem(_, d1, d2) => {
                if index < d1.len() {
                    &mut d1[index]
                } else {
                    &mut d2[index - d1.len()]
                }
            }
        }
    }
}

impl Display for NoirTypeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NoirTypeKind::Int32 => write!(f, "Int32"),
            NoirTypeKind::Float32 => write!(f, "Float32"),
            NoirTypeKind::Bool => write!(f, "Bool"),
            NoirTypeKind::NaN => write!(f, "NaN"),
            NoirTypeKind::None => write!(f, "None"),
        }
    }
}

impl Schema {
    pub fn new(columns: Vec<NoirTypeKind>) -> Self {
        Self { columns }
    }

    pub fn same_type(n_columns: usize, t: NoirTypeKind) -> Self {
        Self {
            columns: (0..n_columns).map(|_| t).collect(),
        }
    }

    pub fn infer_from_file(path: PathBuf) -> Self {
        info!("Infering schema from file: {:?}", path);
        let mut csv_reader = ReaderBuilder::new().from_path(path).unwrap();
        let mut record = csv::StringRecord::new();
        let _ = csv_reader.read_record(&mut record);
        let columns = record
            .iter()
            .map(|item| {
                if item.parse::<i32>().is_ok() {
                    NoirTypeKind::Int32
                } else if item.parse::<f32>().is_ok() {
                    NoirTypeKind::Float32
                } else if item.parse::<bool>().is_ok() {
                    NoirTypeKind::Bool
                } else {
                    NoirTypeKind::None
                }
            })
            .collect();
        Self { columns }
    }

    pub(crate) fn merge(self, other: Schema) -> Schema {
        Schema {
            columns: [self.columns, other.columns].concat(),
        }
    }
}
