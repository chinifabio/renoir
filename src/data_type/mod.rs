use std::{fmt::Display, path::PathBuf};

use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};

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
pub enum NoirTypeRef {
    Int32,
    Float32,
    Bool,
    NaN,
    None,
}

impl Display for NoirTypeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NoirTypeRef::Int32 => write!(f, "Int32"),
            NoirTypeRef::Float32 => write!(f, "Float32"),
            NoirTypeRef::Bool => write!(f, "Bool"),
            NoirTypeRef::NaN => write!(f, "NaN"),
            NoirTypeRef::None => write!(f, "None"),
        }
    }
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq, Hash)]
pub struct Schema {
    pub(crate) columns: Vec<NoirTypeRef>,
}

impl Schema {
    pub fn new(columns: Vec<NoirTypeRef>) -> Self {
        Self { columns }
    }

    pub fn same_type(n_columns: usize, t: NoirTypeRef) -> Self {
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
                    NoirTypeRef::Int32
                } else if item.parse::<f32>().is_ok() {
                    NoirTypeRef::Float32
                } else if item.parse::<bool>().is_ok() {
                    NoirTypeRef::Bool
                } else {
                    NoirTypeRef::None
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
