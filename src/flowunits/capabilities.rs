use std::{collections::HashMap};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Literal {
    Float(f64),
    Str(String),
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Float(l0), Self::Float(r0)) => l0 == r0,
            (Self::Str(l0), Self::Str(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl Eq for Literal {}

#[derive(Clone, Debug)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Clone, Debug, Default)]
pub enum SpecNode {
    Literal(Literal),
    Name(&'static str),
    Op {
        left: Box<SpecNode>,
        op: BinaryOp,
        right: Box<SpecNode>,
    },
    And {
        left: Box<SpecNode>,
        right: Box<SpecNode>,
    },
    Or {
        left: Box<SpecNode>,
        right: Box<SpecNode>,
    },
    #[default]
    Empty,
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Float(value) => write!(f, "{}", value),
            Literal::Str(value) => write!(f, "{}", value),
        }
    }
}

impl std::fmt::Display for SpecNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpecNode::Literal(value) => write!(f, "{}", value),
            SpecNode::Name(name) => write!(f, "{}", name),
            SpecNode::Op { left, op, right } => match op {
                BinaryOp::Eq => write!(f, "({} == {})", left, right),
                BinaryOp::NotEq => write!(f, "({} != {})", left, right),
                BinaryOp::Lt => write!(f, "({} < {})", left, right),
                BinaryOp::LtEq => write!(f, "({} <= {})", left, right),
                BinaryOp::Gt => write!(f, "({} > {})", left, right),
                BinaryOp::GtEq => write!(f, "({} >= {})", left, right),
            },
            SpecNode::And { left, right } => write!(f, "({} && {})", left, right),
            SpecNode::Or { left, right } => write!(f, "({} || {})", left, right),
            SpecNode::Empty => write!(f, "empty"),
        }
    }
}

impl<'a> From<&'a str> for Literal {
    fn from(value: &'a str) -> Self {
        Literal::Str(value.to_string())
    }
}
impl From<i64> for Literal {
    fn from(value: i64) -> Self {
        Literal::Float(value as f64)
    }
}
impl From<f64> for Literal {
    fn from(value: f64) -> Self {
        Literal::Float(value)
    }
}
impl From<Literal> for SpecNode {
    fn from(value: Literal) -> Self {
        SpecNode::Literal(value)
    }
}

pub fn s(spec: &'static str) -> SpecNode {
    SpecNode::Name(spec)
}
pub fn none() -> SpecNode {
    SpecNode::Empty
}

impl SpecNode {
    pub fn eq(self, other: impl Into<Literal>) -> Self {
        SpecNode::Op {
            left: Box::new(self),
            op: BinaryOp::Eq,
            right: Box::new(other.into().into()),
        }
    }

    pub fn not_eq(self, other: impl Into<Literal>) -> Self {
        SpecNode::Op {
            left: Box::new(self),
            op: BinaryOp::NotEq,
            right: Box::new(other.into().into()),
        }
    }

    pub fn ge(self, other: impl Into<Literal>) -> Self {
        SpecNode::Op {
            left: Box::new(self),
            op: BinaryOp::GtEq,
            right: Box::new(other.into().into()),
        }
    }

    pub fn le(self, other: impl Into<Literal>) -> Self {
        SpecNode::Op {
            left: Box::new(self),
            op: BinaryOp::LtEq,
            right: Box::new(other.into().into()),
        }
    }
    pub fn gt(self, other: impl Into<Literal>) -> Self {
        SpecNode::Op {
            left: Box::new(self),
            op: BinaryOp::Gt,
            right: Box::new(other.into().into()),
        }
    }
    pub fn lt(self, other: impl Into<Literal>) -> Self {
        SpecNode::Op {
            left: Box::new(self),
            op: BinaryOp::Lt,
            right: Box::new(other.into().into()),
        }
    }
    pub fn and(self, other: impl Into<SpecNode>) -> Self {
        SpecNode::And {
            left: Box::new(self),
            right: Box::new(other.into()),
        }
    }
    pub fn or(self, other: impl Into<SpecNode>) -> Self {
        SpecNode::Or {
            left: Box::new(self),
            right: Box::new(other.into()),
        }
    }
    
    pub(crate) fn eval(&self, capabilites: &HashMap<String, Literal>) -> Result<bool, String> {
        match self {
            SpecNode::Literal(l) => Err(format!("Literal {l} should not be here")),
            SpecNode::Name(n) => Err(format!("Requirement name {n} should not be here")),
            SpecNode::Op { left, op, right } => {
                let left_value = match **left {
                    SpecNode::Name(key) => key.to_string(),
                    _ => return Err("Requirement needs to have a name on the left side, use s(\"...\")".to_string()),
                };
                let right_value = match &**right {
                    SpecNode::Literal(value) => value.clone(),
                    _ => return Err("Requirement needs to have a literal on the right side".to_string()),
                };
                let left_value = match capabilites.get(&left_value) {
                    Some(Literal::Float(value)) => Literal::Float(*value),
                    Some(Literal::Str(value)) => Literal::Str(value.clone()),
                    None => {
                        log::warn!("Capability {left_value} not found");
                        return Ok(false);
                    },
                };
                match op {
                    BinaryOp::Eq => Ok(left_value.eq(&right_value)),
                    BinaryOp::NotEq => Ok(left_value.ne(&right_value)),
                    BinaryOp::Lt => match (left_value, right_value) {
                        (Literal::Float(l), Literal::Float(r)) => Ok(l < r),
                        _ => Err("Type mismatch".to_string()),
                    },
                    BinaryOp::LtEq => match (left_value, right_value) {
                        (Literal::Float(l), Literal::Float(r)) => Ok(l <= r),
                        _ => Err("Type mismatch".to_string()),
                    },
                    BinaryOp::Gt => match (left_value, right_value) {
                        (Literal::Float(l), Literal::Float(r)) => Ok(l > r),
                        _ => Err("Type mismatch".to_string()),
                    },
                    BinaryOp::GtEq => match (left_value, right_value) {
                        (Literal::Float(l), Literal::Float(r)) => Ok(l >= r),
                        _ => Err("Type mismatch".to_string()),
                    },
                }
            }
            SpecNode::And { left, right } => Ok(left.eval(capabilites)? && right.eval(capabilites)?),
            SpecNode::Or { left, right } => Ok(left.eval(capabilites)? || right.eval(capabilites)?),
            SpecNode::Empty => Ok(true),
        }
    }
}
