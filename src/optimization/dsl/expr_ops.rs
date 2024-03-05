use std::ops::{Add, Div, Mul, Rem, Sub};
use std::ops::{BitAnd, BitOr, BitXor};

use crate::data_type::noir_type::NoirType;

use super::expressions::*;

impl From<i32> for Expr {
    fn from(i: i32) -> Self {
        lit(NoirType::Int32(i))
    }
}

impl From<f32> for Expr {
    fn from(f: f32) -> Self {
        lit(NoirType::Float32(f))
    }
}

impl From<bool> for Expr {
    fn from(b: bool) -> Self {
        lit(NoirType::Bool(b))
    }
}

// ----- Add -----

impl Add for Expr {
    type Output = Expr;

    fn add(self, rhs: Expr) -> Self::Output {
        binary_expr(self, BinaryOp::Plus, rhs)
    }
}

impl Add<f32> for Expr {
    type Output = Expr;

    fn add(self, rhs: f32) -> Self::Output {
        binary_expr(self, BinaryOp::Plus, f(rhs))
    }
}

impl Add<Expr> for f32 {
    type Output = Expr;

    fn add(self, rhs: Expr) -> Self::Output {
        binary_expr(f(self), BinaryOp::Plus, rhs)
    }
}

impl Add<i32> for Expr {
    type Output = Expr;

    fn add(self, rhs: i32) -> Self::Output {
        binary_expr(self, BinaryOp::Plus, lit(NoirType::Int32(rhs)))
    }
}

impl Add<Expr> for i32 {
    type Output = Expr;

    fn add(self, rhs: Expr) -> Self::Output {
        binary_expr(i(self), BinaryOp::Plus, rhs)
    }
}

// ----- Sub -----

impl Sub<Expr> for Expr {
    type Output = Expr;

    fn sub(self, rhs: Expr) -> Self::Output {
        binary_expr(self, BinaryOp::Minus, rhs)
    }
}

impl Sub<i32> for Expr {
    type Output = Expr;

    fn sub(self, rhs: i32) -> Self::Output {
        binary_expr(self, BinaryOp::Minus, i(rhs))
    }
}

impl Sub<Expr> for i32 {
    type Output = Expr;

    fn sub(self, rhs: Expr) -> Self::Output {
        binary_expr(i(self), BinaryOp::Minus, rhs)
    }
}

impl Sub<f32> for Expr {
    type Output = Expr;

    fn sub(self, rhs: f32) -> Self::Output {
        binary_expr(self, BinaryOp::Minus, lit(NoirType::Float32(rhs)))
    }
}

impl Sub<Expr> for f32 {
    type Output = Expr;

    fn sub(self, rhs: Expr) -> Self::Output {
        binary_expr(f(self), BinaryOp::Minus, rhs)
    }
}

// ----- Mul -----

impl Mul for Expr {
    type Output = Expr;

    fn mul(self, rhs: Expr) -> Self::Output {
        binary_expr(self, BinaryOp::Multiply, rhs)
    }
}

impl Mul<i32> for Expr {
    type Output = Expr;

    fn mul(self, rhs: i32) -> Self::Output {
        binary_expr(self, BinaryOp::Multiply, i(rhs))
    }
}

impl Mul<Expr> for i32 {
    type Output = Expr;

    fn mul(self, rhs: Expr) -> Self::Output {
        binary_expr(i(self), BinaryOp::Multiply, rhs)
    }
}

impl Mul<f32> for Expr {
    type Output = Expr;

    fn mul(self, rhs: f32) -> Self::Output {
        binary_expr(self, BinaryOp::Multiply, f(rhs))
    }
}

impl Mul<Expr> for f32 {
    type Output = Expr;

    fn mul(self, rhs: Expr) -> Self::Output {
        binary_expr(f(self), BinaryOp::Multiply, rhs)
    }
}

// ----- Div -----

impl Div for Expr {
    type Output = Expr;

    fn div(self, rhs: Expr) -> Self::Output {
        binary_expr(self, BinaryOp::Divide, rhs)
    }
}

impl Div<i32> for Expr {
    type Output = Expr;

    fn div(self, rhs: i32) -> Self::Output {
        if rhs == 0 {
            panic!("Cannot divide by zero");
        }
        binary_expr(self, BinaryOp::Divide, i(rhs))
    }
}

impl Div<Expr> for i32 {
    type Output = Expr;

    fn div(self, rhs: Expr) -> Self::Output {
        binary_expr(i(self), BinaryOp::Divide, rhs)
    }
}

impl Div<f32> for Expr {
    type Output = Expr;

    fn div(self, rhs: f32) -> Self::Output {
        if rhs == 0.0 {
            panic!("Cannot divide by zero");
        }
        binary_expr(self, BinaryOp::Divide, f(rhs))
    }
}

impl Div<Expr> for f32 {
    type Output = Expr;

    fn div(self, rhs: Expr) -> Self::Output {
        binary_expr(f(self), BinaryOp::Divide, rhs)
    }
}

// ----- Mod -----

impl<T: Into<Expr>> Rem<T> for Expr {
    type Output = Expr;

    fn rem(self, rhs: T) -> Self::Output {
        binary_expr(self, BinaryOp::Mod, rhs.into())
    }
}

impl BitOr for Expr {
    type Output = Expr;

    fn bitor(self, rhs: Expr) -> Self::Output {
        binary_expr(self, BinaryOp::Or, rhs)
    }
}

impl BitAnd for Expr {
    type Output = Expr;

    fn bitand(self, rhs: Expr) -> Self::Output {
        binary_expr(self, BinaryOp::And, rhs)
    }
}

impl BitXor for Expr {
    type Output = Expr;

    fn bitxor(self, rhs: Expr) -> Self::Output {
        binary_expr(self, BinaryOp::Xor, rhs)
    }
}
