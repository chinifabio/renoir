use serde::{Deserialize, Serialize};
use std::cmp::Eq;
use std::f32;
use std::ops::{Add, Div, Mul};

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub enum NoirType {
    Int32(i32),
    Float32(f32),
    NaN(),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Row {
    pub columns: Vec<NoirType>,
}

impl Row {
    pub fn new(columns: Vec<NoirType>) -> Row {
        Row { columns }
    }

    pub fn new_empty() -> Row {
        Row { columns: vec![] }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

#[allow(dead_code)]
impl NoirType {
    fn sqrt(self) -> NoirType {
        match self {
            NoirType::Int32(a) => NoirType::Float32((a as f32).sqrt()),
            NoirType::Float32(a) => NoirType::Float32(a.sqrt()),
            NoirType::NaN() => panic!("Found NaN!"),
        }
    }

    pub fn is_nan(&self) -> bool {
        matches!(self, NoirType::NaN())
    }
}

macro_rules! impl_from {
    ($t:ty, $v:ident) => {
        impl From<$t> for NoirType {
            fn from(item: $t) -> Self {
                NoirType::$v(item)
            }
        }
    };
}

impl_from!(i32, Int32);

impl From<f32> for NoirType {
    fn from(item: f32) -> Self {
        if item.is_finite() {
            NoirType::Float32(item)
        } else {
            NoirType::NaN()
        }
    }
}

impl Mul for NoirType {
    type Output = NoirType;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Int32(a * b),
            (NoirType::Float32(a), NoirType::Float32(b)) => NoirType::Float32(a * b),
            (_, _) => panic!("Type mismatch!"),
        }
    }
}

impl Mul<f32> for NoirType {
    type Output = NoirType;

    fn mul(self, rhs: f32) -> Self::Output {
        match self {
            NoirType::Int32(a) => NoirType::Float32((a as f32) * rhs),
            NoirType::Float32(a) => NoirType::Float32(a * rhs),
            NoirType::NaN() => panic!("Found NaN!"),
        }
    }
}

impl Div<Self> for NoirType {
    type Output = NoirType;

    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Float32(a as f32 / b as f32),
            (NoirType::Float32(a), NoirType::Float32(b)) => NoirType::Float32(a / b),
            (_, _) => panic!("Type mismatch!"),
        }
    }
}

impl Div<f32> for NoirType {
    type Output = NoirType;

    fn div(self, rhs: f32) -> Self::Output {
        match self {
            NoirType::Int32(a) => NoirType::Float32((a as f32) / rhs),
            NoirType::Float32(a) => NoirType::Float32(a / rhs),
            NoirType::NaN() => panic!("Found NaN!"),
        }
    }
}

impl Add for NoirType {
    type Output = NoirType;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Int32(a + b),
            (NoirType::Float32(a), NoirType::Float32(b)) => NoirType::Float32(a + b),
            (_, _) => panic!("Type mismatch!"),
        }
    }
}

impl PartialOrd for NoirType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (NoirType::Int32(a), NoirType::Int32(b)) => a.partial_cmp(b),
            (NoirType::Float32(a), NoirType::Float32(b)) => a.partial_cmp(b),
            (_, _) => panic!("Type mismatch!"),
        }
    }
}

impl Ord for NoirType {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (NoirType::Int32(a), NoirType::Int32(b)) => a.cmp(b),
            (NoirType::Float32(a), NoirType::Float32(b)) => {
                a.partial_cmp(b).unwrap_or_else(|| panic!("Found NaN!"))
            }
            (_, _) => panic!("Type mismatch!"),
        }
    }
}

impl Eq for NoirType {}
impl Eq for Row {}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::NoirType;

    #[test]
    fn test_ord() {
        let f_a = NoirType::Float32(2.0);
        let f_b = NoirType::Float32(3.0);
        let f_c = NoirType::Float32(3.0);

        assert_eq!(f_a.cmp(&f_b), Ordering::Less);
        assert_eq!(f_b.cmp(&f_a), Ordering::Greater);
        assert_eq!(f_b.cmp(&f_c), Ordering::Equal);

        let i_a = NoirType::Int32(2);
        let i_b = NoirType::Int32(3);
        let i_c = NoirType::Int32(3);

        assert_eq!(i_a.cmp(&i_b), Ordering::Less);
        assert_eq!(i_b.cmp(&i_a), Ordering::Greater);
        assert_eq!(i_b.cmp(&i_c), Ordering::Equal);
    }

    #[test]
    fn test_add() {
        let f_a = NoirType::Float32(2.1);
        let f_b = NoirType::Float32(3.0);

        assert_eq!(f_a + f_b, NoirType::Float32(5.1));

        let i_a = NoirType::Int32(2);
        let i_b = NoirType::Int32(3);

        assert_eq!(i_a + i_b, NoirType::Int32(5));
    }

    #[test]
    fn test_div() {
        let f_a = NoirType::Float32(6.6);
        let f_b = NoirType::Float32(3.3);

        assert_eq!(f_a.clone() / f_b, NoirType::Float32(2.0));
        assert_eq!(f_a / 3.3, NoirType::Float32(2.0));

        let i_a = NoirType::Int32(6);
        let i_b = NoirType::Int32(3);

        assert_eq!(i_a.clone() / i_b, NoirType::Float32(2.0));
        assert_eq!(i_a / 3.0, NoirType::Float32(2.0));
    }

    #[test]
    fn test_mul() {
        let f_a = NoirType::Float32(2.0);
        let f_b = NoirType::Float32(3.1);

        assert_eq!(f_a.clone() * f_b, NoirType::Float32(6.2));
        assert_eq!(f_a * 3.1, NoirType::Float32(6.2));

        let i_a = NoirType::Int32(6);
        let i_b = NoirType::Int32(3);

        assert_eq!(i_a.clone() * i_b, NoirType::Int32(18));
        assert_eq!(i_a * 3.0, NoirType::Float32(18.0));
    }

    #[test]
    fn test_sqrt() {
        let f_a = NoirType::Float32(9.0);

        assert_eq!(f_a.sqrt(), NoirType::Float32(3.0));

        let i_a = NoirType::Int32(9);

        assert_eq!(i_a.sqrt(), NoirType::Float32(3.0));
    }
}
