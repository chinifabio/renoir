use serde::{Deserialize, Serialize};

mod noir_type_op;
mod noir_deserialize;
mod noir_data_op;

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Copy)]
pub enum NoirType {
    Int32(i32),
    Float32(f32),
    NaN(),
    None(),
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "type")]
pub enum NoirData {
    Row(Vec<NoirType>),
    NoirType(NoirType),
}
