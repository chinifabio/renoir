use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize};
use std::cmp::Eq;
use std::f32;
use std::fmt;
use std::ops::{Add, AddAssign, Div, DivAssign, Mul, Neg, Sub, SubAssign};

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

impl<'de> Deserialize<'de> for NoirData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum __Field {
            Row,
            NoirType,
        }

        struct __NoirDataVisitor;

        impl<'de> Visitor<'de> for __NoirDataVisitor {
            type Value = NoirData;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a struct NoirData")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut data: Vec<NoirType> = Vec::new();

                while let Ok(Some(value)) = seq.next_element::<String>() {
                    if let Ok(int_value) = value.parse::<i32>() {
                        data.push(NoirType::Int32(int_value));
                    } else if let Ok(float_value) = value.parse::<f32>() {
                        data.push(NoirType::Float32(float_value));
                    } else {
                        data.push(NoirType::None());
                    }
                }

                if data.len() == 1 {
                    Ok(NoirData::NoirType(data.remove(0)))
                } else {
                    Ok(NoirData::Row(data))
                }
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut data: Vec<NoirType> = Vec::new();

                while let Ok(value) = map.next_value::<String>() {
                    if let Ok(int_value) = value.parse::<i32>() {
                        data.push(NoirType::Int32(int_value));
                    } else if let Ok(float_value) = value.parse::<f32>() {
                        data.push(NoirType::Float32(float_value));
                    } else {
                        data.push(NoirType::None());
                    }
                }

                if data.len() == 1 {
                    Ok(NoirData::NoirType(data.remove(0)))
                } else {
                    Ok(NoirData::Row(data))
                }
            }
        }

        deserializer.deserialize_map(__NoirDataVisitor {})
    }
}

impl NoirData {
    pub fn new(columns: Vec<NoirType>) -> NoirData {
        NoirData::Row(columns)
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
}

#[allow(dead_code)]
impl NoirType {
    fn sqrt(self) -> NoirType {
        match self {
            NoirType::Int32(a) => NoirType::Float32((a as f32).sqrt()),
            NoirType::Float32(a) => NoirType::Float32(a.sqrt()),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        }
    }

    pub fn is_nan(&self) -> bool {
        matches!(self, NoirType::NaN())
    }

    pub fn is_none(&self) -> bool {
        matches!(self, NoirType::None())
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

macro_rules! impl_from_option {
    ($t:ty, $v:ident) => {
        impl From<Option<$t>> for NoirType {
            fn from(item: Option<$t>) -> Self {
                match item {
                    Some(i) => NoirType::$v(i),
                    None => NoirType::None(),
                }
            }
        }
    };
}

impl_from_option!(i32, Int32);
impl_from_option!(f32, Float32);

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
            NoirType::None() => panic!("Found None!"),
        }
    }
}

impl DivAssign<usize> for NoirType {
    fn div_assign(&mut self, rhs: usize) {
        match self {
            NoirType::Int32(a) => *a /= rhs as i32,
            NoirType::Float32(a) => *a /= rhs as f32,
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        }
    }
}

impl DivAssign<Self> for NoirType {
    fn div_assign(&mut self, rhs: Self) {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => *a /= b,
            (NoirType::Float32(a), NoirType::Float32(b)) => *a /= b,
            (_, _) => panic!("Type mismatch!"),
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

impl Div<usize> for NoirType {
    type Output = NoirType;

    fn div(self, rhs: usize) -> Self::Output {
        match self {
            NoirType::Int32(a) => NoirType::Float32((a as f32) / (rhs as f32)),
            NoirType::Float32(a) => NoirType::Float32(a / (rhs as f32)),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
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
            NoirType::None() => panic!("Found None!"),
        }
    }
}

impl Add<&Self> for NoirType {
    type Output = NoirType;

    fn add(self, rhs: &Self) -> Self::Output {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Int32(a + b),
            (NoirType::Float32(a), NoirType::Float32(b)) => NoirType::Float32(a + b),
            (_, _) => panic!("Type mismatch!"),
        }
    }
}

impl AddAssign for NoirType {
    fn add_assign(&mut self, rhs: Self) {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => *a += b,
            (NoirType::Float32(a), NoirType::Float32(b)) => *a += b,
            (_, _) => panic!("Type mismatch!"),
        }
    }
}

impl Add<Self> for NoirType {
    type Output = NoirType;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Int32(a + b),
            (NoirType::Float32(a), NoirType::Float32(b)) => NoirType::Float32(a + b),
            (_, _) => panic!("Type mismatch!"),
        }
    }
}

impl Sub<&Self> for NoirType {
    type Output = NoirType;

    fn sub(self, rhs: &Self) -> Self::Output {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Int32(a - b),
            (NoirType::Float32(a), NoirType::Float32(b)) => NoirType::Float32(a - b),
            (_, _) => panic!("Type mismatch!"),
        }
    }
}

impl SubAssign for NoirType {
    fn sub_assign(&mut self, rhs: Self) {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => *a -= b,
            (NoirType::Float32(a), NoirType::Float32(b)) => *a -= b,
            (_, _) => panic!("Type mismatch!"),
        }
    }
}

impl Sub<Self> for NoirType {
    type Output = NoirType;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Int32(a - b),
            (NoirType::Float32(a), NoirType::Float32(b)) => NoirType::Float32(a - b),
            (_, _) => panic!("Type mismatch!"),
        }
    }
}

impl Neg for NoirType {
    type Output = NoirType;

    fn neg(self) -> Self::Output {
        match self {
            NoirType::Int32(a) => NoirType::Int32(-a),
            NoirType::Float32(a) => NoirType::Float32(-a),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
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

impl Eq for NoirType {}
impl Eq for NoirData {}

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

        assert_eq!(f_a / f_b, NoirType::Float32(2.0));
        assert_eq!(f_a / 3.3, NoirType::Float32(2.0));

        let i_a = NoirType::Int32(6);
        let i_b = NoirType::Int32(3);

        assert_eq!(i_a / i_b, NoirType::Float32(2.0));
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
