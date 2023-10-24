use serde::{Deserialize, Serialize};

mod noir_data_op;
mod noir_deserialize;
mod noir_type_op;
mod greenwald_khanna;

/// NoirType is the basic data type in Noir.
/// It can be either an Int32 or a Float32.
/// NaN defines a value that cannot be used in any calculation and the operators should be able to handle it.
/// None defines a missing value.
#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Copy)]
pub enum NoirType {
    Int32(i32),
    Float32(f32),
    NaN(),
    None(),
}

/// NoirData is the data type that is used in Noir.
/// It can be either a row of NoirType or a single NoirType, this reduce the allocation of memory necessary
/// when we are dealing with a single value.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum NoirData {
    Row(Vec<NoirType>),
    NoirType(NoirType),
}

#[derive(Clone, Debug)]
pub enum NoirDataCsv {
    Row(Vec<NoirType>),
    NoirType(NoirType),
}
