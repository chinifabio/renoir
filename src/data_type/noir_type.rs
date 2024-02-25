use core::panic;
use std::cmp::Eq;
use std::f32;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::{Add, AddAssign, BitXor, Div, DivAssign, Mul, Neg, Not, Rem, Sub, SubAssign};

use serde::{Deserialize, Serialize};
use sha2::digest::typenum::Pow;

use crate::data_type::noir_data::NoirData;

impl NoirType {
    pub fn sqrt(self) -> NoirType {
        let res = match self {
            NoirType::Int32(a) => NoirType::Float32((a as f32).sqrt()),
            NoirType::Float32(a) => NoirType::Float32(a.sqrt()),
            NoirType::Bool(_a) => panic!("Found Bool!"),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        };

        if res == NoirType::Float32(f32::NAN) {
            NoirType::NaN()
        } else {
            res
        }
    }

    pub fn floor(self) -> NoirType {
        match self {
            NoirType::Int32(a) => NoirType::Int32(a),
            NoirType::Float32(a) => NoirType::Float32(a.floor()),
            NoirType::Bool(_a) => panic!("Found Bool!"),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        }
    }

    pub fn ceil(self) -> NoirType {
        match self {
            NoirType::Int32(a) => NoirType::Int32(a),
            NoirType::Float32(a) => NoirType::Float32(a.ceil()),
            NoirType::Bool(_a) => panic!("Found Bool!"),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        }
    }

    pub fn round(self) -> NoirType {
        match self {
            NoirType::Int32(a) => NoirType::Int32(a),
            NoirType::Float32(a) => NoirType::Float32(a.round()),
            NoirType::Bool(_a) => panic!("Found Bool!"),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        }
    }

    pub fn modulo(self, n: i32) -> NoirType {
        match self {
            NoirType::Int32(a) => NoirType::Int32(a % n),
            NoirType::Float32(a) => NoirType::Float32(a % n as f32),
            NoirType::Bool(_a) => panic!("Found Bool!"),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        }
    }

    pub fn abs(self) -> NoirType {
        match self {
            NoirType::Int32(a) => NoirType::Int32(a.abs()),
            NoirType::Float32(a) => NoirType::Float32(a.abs()),
            NoirType::Bool(_a) => panic!("Found Bool!"),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        }
    }

    pub fn is_na(&self) -> bool {
        matches!(self, NoirType::NaN()) || matches!(self, NoirType::None())
    }

    pub fn is_nan(&self) -> bool {
        matches!(self, NoirType::NaN())
    }

    pub fn is_none(&self) -> bool {
        matches!(self, NoirType::None())
    }

    pub fn or(self, other: &NoirType) -> NoirType {
        match self {
            NoirType::None() => *other,
            _ => self,
        }
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
impl_from!(bool, Bool);

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
impl_from_option!(bool, Bool);

impl From<f32> for NoirType {
    fn from(item: f32) -> Self {
        if item.is_finite() {
            NoirType::Float32(item)
        } else {
            NoirType::NaN()
        }
    }
}

impl Hash for NoirType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            NoirType::Int32(i) => i.hash(state),
            NoirType::Float32(f) => f.to_bits().hash(state),
            NoirType::Bool(b) => b.hash(state),
            NoirType::NaN() => (),
            NoirType::None() => (),
        }
    }
}

impl From<NoirData> for NoirType {
    fn from(val: NoirData) -> Self {
        match val {
            NoirData::Row(_) => panic!("Cannot convert row into NoirType!"),
            NoirData::NoirType(t) => t,
        }
    }
}

impl From<NoirType> for f64 {
    fn from(value: NoirType) -> Self {
        match value {
            NoirType::Int32(i) => i as f64,
            NoirType::Float32(f) => f as f64,
            NoirType::Bool(b) => {
                if b {
                    1.0
                } else {
                    0.0
                }
            }
            NoirType::NaN() => f64::NAN,
            NoirType::None() => f64::NAN,
        }
    }
}

impl Mul for NoirType {
    type Output = NoirType;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Int32(a * b),
            (NoirType::Float32(a), NoirType::Float32(b)) => NoirType::Float32(a * b),
            (NoirType::Float32(a), NoirType::Int32(b)) => NoirType::Float32(a * b as f32),
            (NoirType::Int32(a), NoirType::Float32(b)) => NoirType::Float32(a as f32 * b),
            (_, _) => panic!("Type mismatch!"),
        }
    }
}

impl Mul<f32> for NoirType {
    type Output = NoirType;

    fn mul(self, rhs: f32) -> Self::Output {
        let res = match self {
            NoirType::Int32(a) => NoirType::Float32((a as f32) * rhs),
            NoirType::Float32(a) => NoirType::Float32(a * rhs),
            NoirType::Bool(a) => NoirType::Float32(if a { rhs } else { 0.0 }),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        };

        if res == NoirType::Float32(f32::NAN) {
            NoirType::NaN()
        } else {
            res
        }
    }
}

impl Mul<NoirType> for i32 {
    type Output = NoirType;

    fn mul(self, rhs: NoirType) -> Self::Output {
        match rhs {
            NoirType::Int32(a) => NoirType::Int32(a * self),
            NoirType::Float32(a) => NoirType::Float32(a * self as f32),
            NoirType::Bool(a) => NoirType::Int32(if a { self } else { 0 }),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        }
    }
}

impl Mul<i32> for NoirType {
    type Output = NoirType;

    fn mul(self, rhs: i32) -> Self::Output {
        match self {
            NoirType::Int32(a) => NoirType::Int32(a * rhs),
            NoirType::Float32(a) => NoirType::Float32(a * rhs as f32),
            NoirType::Bool(a) => NoirType::Int32(if a { rhs } else { 0 }),
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
            NoirType::Bool(_a) => panic!("Found Bool!"),
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
        let res = match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Float32(a as f32 / b as f32),
            (NoirType::Float32(a), NoirType::Float32(b)) => NoirType::Float32(a / b),
            (NoirType::Float32(a), NoirType::Int32(b)) => NoirType::Float32(a / b as f32),
            (NoirType::Int32(a), NoirType::Float32(b)) => NoirType::Float32(a as f32 / b),
            (_, _) => panic!("NaN or None!"),
        };

        if res == NoirType::Float32(f32::NAN) {
            NoirType::NaN()
        } else {
            res
        }
    }
}

impl Div<&Self> for NoirType {
    type Output = NoirType;

    fn div(self, rhs: &Self) -> Self::Output {
        let res = match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Float32(a as f32 / *b as f32),
            (NoirType::Float32(a), NoirType::Float32(b)) => NoirType::Float32(a / b),
            (NoirType::Float32(a), NoirType::Int32(b)) => NoirType::Float32(a / *b as f32),
            (NoirType::Int32(a), NoirType::Float32(b)) => NoirType::Float32(a as f32 / b),
            (_, _) => panic!("NaN or None!"),
        };

        if res == NoirType::Float32(f32::NAN) {
            NoirType::NaN()
        } else {
            res
        }
    }
}

impl Div<usize> for NoirType {
    type Output = NoirType;

    fn div(self, rhs: usize) -> Self::Output {
        let res = match self {
            NoirType::Int32(a) => NoirType::Float32((a as f32) / (rhs as f32)),
            NoirType::Float32(a) => NoirType::Float32(a / (rhs as f32)),
            NoirType::Bool(_a) => panic!("Found Bool!"),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        };

        if res == NoirType::Float32(f32::NAN) {
            NoirType::NaN()
        } else {
            res
        }
    }
}

impl Div<f32> for NoirType {
    type Output = NoirType;

    fn div(self, rhs: f32) -> Self::Output {
        let res = match self {
            NoirType::Int32(a) => NoirType::Float32((a as f32) / rhs),
            NoirType::Float32(a) => NoirType::Float32(a / rhs),
            NoirType::Bool(_a) => panic!("Found Bool!"),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        };

        if res == NoirType::Float32(f32::NAN) {
            NoirType::NaN()
        } else {
            res
        }
    }
}

impl Div<f64> for NoirType {
    type Output = NoirType;

    fn div(self, rhs: f64) -> Self::Output {
        let res = match self {
            NoirType::Int32(a) => NoirType::Float32((a as f32) / rhs as f32),
            NoirType::Float32(a) => NoirType::Float32(a / rhs as f32),
            NoirType::Bool(_a) => panic!("Found Bool!"),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        };

        if res == NoirType::Float32(f32::NAN) {
            NoirType::NaN()
        } else {
            res
        }
    }
}

impl Pow<i32> for NoirType {
    type Output = NoirType;

    fn powi(self, exp: i32) -> Self::Output {
        match self {
            NoirType::Int32(a) => NoirType::Float32((a as f32).powi(exp)),
            NoirType::Float32(a) => NoirType::Float32(a.powi(exp)),
            NoirType::Bool(_a) => panic!("Found Bool!"),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        }
    }
}

impl Add<i32> for NoirType {
    type Output = NoirType;

    fn add(self, rhs: i32) -> Self::Output {
        match self {
            NoirType::Int32(a) => NoirType::Int32(a + rhs),
            NoirType::Float32(a) => NoirType::Float32(a + rhs as f32),
            NoirType::Bool(_a) => panic!("Found Bool!"),
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
            (NoirType::Int32(a), NoirType::Float32(b)) => NoirType::Float32(a as f32 + b),
            (NoirType::Float32(a), NoirType::Int32(b)) => NoirType::Float32(a + *b as f32),
            (_, _) => panic!("None or NaN!"),
        }
    }
}

impl AddAssign for NoirType {
    fn add_assign(&mut self, rhs: Self) {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => *a += b,
            (NoirType::Float32(a), NoirType::Float32(b)) => *a += b,
            (NoirType::Float32(a), NoirType::Int32(b)) => *a += b as f32,
            (NoirType::Int32(_), NoirType::Float32(_)) => panic!("Convert data to float!"),
            (_, _) => panic!("NaN or None!"),
        }
    }
}

impl Add<Self> for NoirType {
    type Output = NoirType;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Int32(a + b),
            (NoirType::Float32(a), NoirType::Float32(b)) => NoirType::Float32(a + b),
            (NoirType::Float32(a), NoirType::Int32(b)) => NoirType::Float32(a + b as f32),
            (NoirType::Int32(a), NoirType::Float32(b)) => NoirType::Float32(a as f32 + b),
            (_, _) => panic!("None or NaN!"),
        }
    }
}

impl Sub<&Self> for NoirType {
    type Output = NoirType;

    fn sub(self, rhs: &Self) -> Self::Output {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Int32(a - b),
            (NoirType::Float32(a), NoirType::Float32(b)) => NoirType::Float32(a - b),
            (NoirType::Float32(a), NoirType::Int32(b)) => NoirType::Float32(a - *b as f32),
            (NoirType::Int32(a), NoirType::Float32(b)) => NoirType::Float32(a as f32 - b),
            (_, _) => panic!("NaN or None!"),
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
            (NoirType::Float32(a), NoirType::Int32(b)) => NoirType::Float32(a - b as f32),
            (NoirType::Int32(a), NoirType::Float32(b)) => NoirType::Float32(a as f32 - b),
            (_, _) => panic!("NaN or None!"),
        }
    }
}

impl Sub<i32> for NoirType {
    type Output = NoirType;

    fn sub(self, rhs: i32) -> Self::Output {
        match self {
            NoirType::Int32(a) => NoirType::Int32(a - rhs),
            NoirType::Float32(a) => NoirType::Float32(a - rhs as f32),
            NoirType::Bool(_a) => panic!("Found Bool!"),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        }
    }
}

impl Neg for NoirType {
    type Output = NoirType;

    fn neg(self) -> Self::Output {
        match self {
            NoirType::Int32(a) => NoirType::Int32(-a),
            NoirType::Float32(a) => NoirType::Float32(-a),
            NoirType::Bool(a) => NoirType::Bool(!a),
            NoirType::NaN() => panic!("Found NaN!"),
            NoirType::None() => panic!("Found None!"),
        }
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
            (NoirType::Int32(a), NoirType::Int32(b)) => a.cmp(b),
            (NoirType::Float32(a), NoirType::Float32(b)) => {
                a.partial_cmp(b).unwrap_or_else(|| panic!("Found NaN!"))
            }
            (NoirType::Float32(a), NoirType::Int32(b)) => a
                .partial_cmp(&(*b as f32))
                .unwrap_or_else(|| panic!("Found NaN!")),
            (NoirType::Int32(a), NoirType::Float32(b)) => match b.partial_cmp(&(*a as f32)) {
                Some(ord) => ord.reverse(),
                None => panic!("Found NaN!"),
            },
            (_, _) => panic!("Found NaN or None!"),
        }
    }
}

impl PartialEq for NoirType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int32(l0), Self::Int32(r0)) => l0 == r0,
            (Self::Float32(l0), Self::Float32(r0)) => l0 == r0,
            (Self::Bool(l0), Self::Bool(r0)) => l0 == r0,
            (Self::NaN(), Self::NaN()) => false,
            (Self::None(), Self::None()) => true,
            (Self::Int32(l0), Self::Float32(r0)) => l0 == &(*r0 as i32),
            (Self::Float32(l0), Self::Int32(r0)) => &(*l0 as i32) == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl Eq for NoirType {}

impl From<NoirType> for bool {
    fn from(value: NoirType) -> Self {
        match value {
            NoirType::Int32(i) => i != 0,
            NoirType::Float32(f) => f != 0.0,
            NoirType::Bool(b) => b,
            NoirType::NaN() => false,
            NoirType::None() => false,
        }
    }
}

impl Not for NoirType {
    type Output = NoirType;

    fn not(self) -> Self::Output {
        match self {
            NoirType::Int32(i) => NoirType::Bool(i == 0),
            NoirType::Float32(f) => NoirType::Bool(f == 0.0),
            NoirType::Bool(b) => NoirType::Bool(!b),
            NoirType::NaN() => NoirType::Bool(true),
            NoirType::None() => NoirType::Bool(true),
        }
    }
}

impl Rem for NoirType {
    type Output = NoirType;

    fn rem(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Int32(a % b),
            (NoirType::Float32(a), NoirType::Int32(b)) => NoirType::Float32(a % b as f32),
            (_, _) => panic!("NaN, None or Float! ({} % {})", self, rhs),
        }
    }
}

impl BitXor for NoirType {
    type Output = NoirType;

    fn bitxor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (NoirType::Int32(a), NoirType::Int32(b)) => NoirType::Int32(a ^ b),
            (NoirType::Bool(a), NoirType::Bool(b)) => NoirType::Bool(a ^ b),
            (_, _) => panic!("Operation not supported! ({} ^ {})", self, rhs),
        }
    }
}

impl Display for NoirType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NoirType::Int32(i) => write!(f, "{}", i),
            NoirType::Float32(i) => write!(f, "{}", i),
            NoirType::Bool(a) => write!(f, "{}", a),
            NoirType::NaN() => write!(f, "NaN"),
            NoirType::None() => write!(f, "None"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use crate::data_type::noir_type::NoirType;

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

        assert_eq!(f_a * f_b, NoirType::Float32(6.2));
        assert_eq!(f_a * 3.1, NoirType::Float32(6.2));

        let i_a = NoirType::Int32(6);
        let i_b = NoirType::Int32(3);

        assert_eq!(i_a * i_b, NoirType::Int32(18));
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

/// NoirType is the basic data type in Noir.
/// It can be either an Int32 or a Float32.
/// NaN defines a value that cannot be used in any calculation and the operators should be able to handle it.
/// None defines a missing value.
#[repr(C)]
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

impl From<NoirType> for NoirTypeKind {
    fn from(item: NoirType) -> Self {
        match item {
            NoirType::Int32(_) => NoirTypeKind::Int32,
            NoirType::Float32(_) => NoirTypeKind::Float32,
            NoirType::Bool(_) => NoirTypeKind::Bool,
            NoirType::NaN() => NoirTypeKind::NaN,
            NoirType::None() => NoirTypeKind::None,
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
