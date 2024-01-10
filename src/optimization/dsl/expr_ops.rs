use std::ops::{Add, Div, Mul, Rem, Sub};
use std::ops::{BitAnd, BitOr, BitXor};

use crate::data_type::NoirType;

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

// ----- Add -----

impl Add for Expr {
    type Output = Expr;

    fn add(self, rhs: Expr) -> Self::Output {
        binary_expr(self, ExprOp::Plus, rhs)
    }
}

impl Add<f32> for Expr {
    type Output = Expr;

    fn add(self, rhs: f32) -> Self::Output {
        binary_expr(self, ExprOp::Plus, f(rhs))
    }
}

impl Add<Expr> for f32 {
    type Output = Expr;

    fn add(self, rhs: Expr) -> Self::Output {
        binary_expr(f(self), ExprOp::Plus, rhs)
    }
}

impl Add<i32> for Expr {
    type Output = Expr;

    fn add(self, rhs: i32) -> Self::Output {
        binary_expr(self, ExprOp::Plus, lit(NoirType::Int32(rhs)))
    }
}

impl Add<Expr> for i32 {
    type Output = Expr;

    fn add(self, rhs: Expr) -> Self::Output {
        binary_expr(i(self), ExprOp::Plus, rhs)
    }
}

// ----- Sub -----

impl Sub<Expr> for Expr {
    type Output = Expr;

    fn sub(self, rhs: Expr) -> Self::Output {
        binary_expr(self, ExprOp::Minus, rhs)
    }
}

impl Sub<i32> for Expr {
    type Output = Expr;

    fn sub(self, rhs: i32) -> Self::Output {
        binary_expr(self, ExprOp::Minus, i(rhs))
    }
}

impl Sub<Expr> for i32 {
    type Output = Expr;

    fn sub(self, rhs: Expr) -> Self::Output {
        binary_expr(i(self), ExprOp::Minus, rhs)
    }
}

impl Sub<f32> for Expr {
    type Output = Expr;

    fn sub(self, rhs: f32) -> Self::Output {
        binary_expr(self, ExprOp::Minus, lit(NoirType::Float32(rhs)))
    }
}

impl Sub<Expr> for f32 {
    type Output = Expr;

    fn sub(self, rhs: Expr) -> Self::Output {
        binary_expr(f(self), ExprOp::Minus, rhs)
    }
}

// ----- Mul -----

impl Mul for Expr {
    type Output = Expr;

    fn mul(self, rhs: Expr) -> Self::Output {
        binary_expr(self, ExprOp::Multiply, rhs)
    }
}

impl Mul<i32> for Expr {
    type Output = Expr;

    fn mul(self, rhs: i32) -> Self::Output {
        binary_expr(self, ExprOp::Multiply, i(rhs))
    }
}

impl Mul<Expr> for i32 {
    type Output = Expr;

    fn mul(self, rhs: Expr) -> Self::Output {
        binary_expr(i(self), ExprOp::Multiply, rhs)
    }
}

impl Mul<f32> for Expr {
    type Output = Expr;

    fn mul(self, rhs: f32) -> Self::Output {
        binary_expr(self, ExprOp::Multiply, f(rhs))
    }
}

impl Mul<Expr> for f32 {
    type Output = Expr;

    fn mul(self, rhs: Expr) -> Self::Output {
        binary_expr(f(self), ExprOp::Multiply, rhs)
    }
}

// ----- Div -----

impl Div for Expr {
    type Output = Expr;

    fn div(self, rhs: Expr) -> Self::Output {
        binary_expr(self, ExprOp::Divide, rhs)
    }
}

impl Div<i32> for Expr {
    type Output = Expr;

    fn div(self, rhs: i32) -> Self::Output {
        if rhs == 0 {
            panic!("Cannot divide by zero");
        }
        binary_expr(self, ExprOp::Divide, i(rhs))
    }
}

impl Div<Expr> for i32 {
    type Output = Expr;

    fn div(self, rhs: Expr) -> Self::Output {
        binary_expr(i(self), ExprOp::Divide, rhs)
    }
}

impl Div<f32> for Expr {
    type Output = Expr;

    fn div(self, rhs: f32) -> Self::Output {
        if rhs == 0.0 {
            panic!("Cannot divide by zero");
        }
        binary_expr(self, ExprOp::Divide, f(rhs))
    }
}

impl Div<Expr> for f32 {
    type Output = Expr;

    fn div(self, rhs: Expr) -> Self::Output {
        binary_expr(f(self), ExprOp::Divide, rhs)
    }
}

// ----- Mod -----

impl Rem for Expr {
    type Output = Expr;

    fn rem(self, rhs: Expr) -> Self::Output {
        binary_expr(self, ExprOp::Mod, rhs)
    }
}

impl BitOr for Expr {
    type Output = Expr;

    fn bitor(self, rhs: Expr) -> Self::Output {
        binary_expr(self, ExprOp::Or, rhs)
    }
}

impl BitAnd for Expr {
    type Output = Expr;

    fn bitand(self, rhs: Expr) -> Self::Output {
        binary_expr(self, ExprOp::And, rhs)
    }
}

impl BitXor for Expr {
    type Output = Expr;

    fn bitxor(self, rhs: Expr) -> Self::Output {
        binary_expr(self, ExprOp::Xor, rhs)
    }
}
