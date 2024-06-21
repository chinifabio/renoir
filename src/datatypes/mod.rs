pub mod arithmetic;
pub mod cmp;
pub mod logic;
pub mod schema;
pub mod stream_item;

use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum NoirType {
    Int(i32),
    Float(f32),
    Bool(bool),
    String(String),
    None(),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash, Copy)]
pub enum NoirTypeKind {
    Int,
    Float,
    Bool,
    String,
    None,
}

impl Display for NoirType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NoirType::Int(i) => write!(f, "{}", i),
            NoirType::Float(fl) => write!(f, "{}", fl),
            NoirType::Bool(b) => write!(f, "{}", b),
            NoirType::String(s) => write!(f, "{}", s),
            NoirType::None() => write!(f, "None"),
        }
    }
}

impl Display for NoirTypeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NoirTypeKind::Int => write!(f, "Int"),
            NoirTypeKind::Float => write!(f, "Float"),
            NoirTypeKind::Bool => write!(f, "Bool"),
            NoirTypeKind::String => write!(f, "String"),
            NoirTypeKind::None => write!(f, "None"),
        }
    }
}

impl From<i32> for NoirType {
    fn from(i: i32) -> Self {
        NoirType::Int(i)
    }
}

impl From<f32> for NoirType {
    fn from(f: f32) -> Self {
        NoirType::Float(f)
    }
}

impl From<bool> for NoirType {
    fn from(b: bool) -> Self {
        NoirType::Bool(b)
    }
}

impl From<String> for NoirType {
    fn from(s: String) -> Self {
        NoirType::String(s)
    }
}

impl From<&str> for NoirType {
    fn from(s: &str) -> Self {
        NoirType::String(s.to_string())
    }
}

impl PartialOrd for NoirType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NoirType {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (NoirType::Int(i1), NoirType::Int(i2)) => i1.cmp(i2),
            (NoirType::Float(f1), NoirType::Float(f2)) => f1.partial_cmp(f2).unwrap(),
            (NoirType::String(s1), NoirType::String(s2)) => s1.cmp(s2),
            (NoirType::None(), NoirType::None()) => std::cmp::Ordering::Equal,
            // TODO: tutte le altre combinazioni
            _ => panic!("Impossible to compare {:?} and {:?}", self, other),
        }
    }
}

impl From<NoirType> for bool {
    fn from(t: NoirType) -> Self {
        match t {
            NoirType::Int(i) => i != 0,
            NoirType::Float(f) => f != 0.0,
            NoirType::Bool(b) => b,
            _ => panic!("Cannot convert {:?} to bool", t),
        }
    }
}

impl PartialEq for NoirType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (NoirType::Int(i1), NoirType::Int(i2)) => i1 == i2,
            (NoirType::Float(f1), NoirType::Float(f2)) => f1 == f2,
            (NoirType::Bool(b1), NoirType::Bool(b2)) => b1 == b2,
            (NoirType::String(s1), NoirType::String(s2)) => s1 == s2,
            (NoirType::None(), NoirType::None()) => true,
            // TODO: tutte le altre combinazioni
            _ => false,
        }
    }
}

impl Eq for NoirType {}

impl core::hash::Hash for NoirType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            NoirType::Int(i) => i.hash(state),
            NoirType::Float(f) => f.to_bits().hash(state),
            NoirType::Bool(b) => b.hash(state),
            NoirType::String(s) => s.hash(state),
            NoirType::None() => 0.hash(state),
        }
    }
}

impl NoirType {
    pub fn sqrt(self) -> NoirType {
        match self {
            NoirType::Int(a) => NoirType::Float((a as f32).sqrt()),
            NoirType::Float(a) => NoirType::Float(a.sqrt()),
            _ => panic!("Impossible to calculate the square root of {:?}", self),
        }
    }

    pub fn floor(self) -> NoirType {
        match self {
            NoirType::Int(a) => NoirType::Int(a),
            NoirType::Float(a) => NoirType::Float(a.floor()),
            _ => panic!("Impossible to floor {:?}", self),
        }
    }

    pub fn ceil(self) -> NoirType {
        match self {
            NoirType::Int(a) => NoirType::Int(a),
            NoirType::Float(a) => NoirType::Float(a.ceil()),
            _ => panic!("Impossible to ceil {:?}", self),
        }
    }

    pub fn round(self) -> NoirType {
        match self {
            NoirType::Int(a) => NoirType::Int(a),
            NoirType::Float(a) => NoirType::Float(a.round()),
            _ => panic!("Impossible to round {:?}", self),
        }
    }

    pub fn modulo(self, n: i32) -> NoirType {
        match self {
            NoirType::Int(a) => NoirType::Int(a % n),
            NoirType::Float(a) => NoirType::Float(a % n as f32),
            _ => panic!("Impossible to calculate the modulo of {:?}", self),
        }
    }

    pub fn abs(self) -> NoirType {
        match self {
            NoirType::Int(a) => NoirType::Int(a.abs()),
            NoirType::Float(a) => NoirType::Float(a.abs()),
            _ => panic!("Impossible to calculate the absolute value of {:?}", self),
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, NoirType::None())
    }

    pub fn or_else(self, other: NoirType) -> NoirType {
        match self {
            NoirType::None() => other,
            _ => self,
        }
    }

    pub(crate) fn kind(&self) -> NoirTypeKind {
        match self {
            NoirType::Int(_) => NoirTypeKind::Int,
            NoirType::Float(_) => NoirTypeKind::Float,
            NoirType::Bool(_) => NoirTypeKind::Bool,
            NoirType::String(_) => NoirTypeKind::String,
            NoirType::None() => NoirTypeKind::None,
        }
    }
}
