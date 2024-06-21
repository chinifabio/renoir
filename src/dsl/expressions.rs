use core::panic;
use std::fmt::{Debug, Display};
use std::ops::Index;

use serde::{Deserialize, Serialize};

use crate::datatypes::schema::Schema;
use crate::datatypes::NoirType;

use super::aggregate::*;
use super::jit::JitCompiler;

#[derive(Clone, PartialEq, Eq, Debug, Copy, Hash)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
    Mod,
    And,
    Or,
    Xor,
}

#[derive(Clone, PartialEq, Eq, Debug, Copy, Hash)]
pub enum UnaryOp {
    Floor,
    Ceil,
    Abs,
    Sqrt,
    Round,
}

#[derive(Clone, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
pub enum AggregateOp {
    Val(Val),
    Sum(Sum),
    Count(Count),
    Min(Min),
    Max(Max),
    Avg(Avg),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ColumnIndex {
    Nth(usize),
    Name(String),
}

impl From<usize> for ColumnIndex {
    fn from(n: usize) -> Self {
        ColumnIndex::Nth(n)
    }
}

impl From<&str> for ColumnIndex {
    fn from(name: &str) -> Self {
        ColumnIndex::Name(name.to_string())
    }
}

impl Display for ColumnIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnIndex::Nth(n) => write!(f, "{}", n),
            ColumnIndex::Name(name) => write!(f, "{}", name),
        }
    }
}

pub type ExprRef = Box<Expr>;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Expr {
    // NthColumn(ColumnIndex),
    NthColumn(usize),
    Literal(NoirType),
    BinaryExpr {
        left: ExprRef,
        op: BinaryOp,
        right: ExprRef,
    },
    UnaryExpr {
        op: UnaryOp,
        expr: ExprRef,
    },
    AggregateExpr {
        op: AggregateOp,
        expr: ExprRef,
    },
    Compiled {
        compiled_eval: fn(*const NoirType) -> NoirType,
        expr: ExprRef,
    },
    Empty,
}

pub enum ArenaExpr {
    NthColumn(usize),
    Literal(NoirType),
    BinaryExpr {
        left: usize,
        op: BinaryOp,
        right: usize,
    },
    UnaryExpr {
        op: UnaryOp,
        expr: usize,
    },
    AggregateExpr {
        op: AggregateOp,
        expr: usize,
    },
    Empty,
}

impl Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::NthColumn(n) => write!(f, "col({})", n),
            Expr::Literal(value) => write!(f, "{}", value),
            Expr::BinaryExpr { left, op, right } => match op {
                BinaryOp::Plus => write!(f, "({} + {})", left, right),
                BinaryOp::Minus => write!(f, "({} - {})", left, right),
                BinaryOp::Multiply => write!(f, "({} * {})", left, right),
                BinaryOp::Divide => write!(f, "({} / {})", left, right),
                BinaryOp::Eq => write!(f, "({} == {})", left, right),
                BinaryOp::And => write!(f, "({} && {})", left, right),
                BinaryOp::NotEq => write!(f, "({} != {})", left, right),
                BinaryOp::Lt => write!(f, "({} < {})", left, right),
                BinaryOp::LtEq => write!(f, "({} <= {})", left, right),
                BinaryOp::Gt => write!(f, "({} > {})", left, right),
                BinaryOp::GtEq => write!(f, "({} >= {})", left, right),
                BinaryOp::Or => write!(f, "({} || {})", left, right),
                BinaryOp::Xor => write!(f, "({} ^ {})", left, right),
                BinaryOp::Mod => write!(f, "({} % {})", left, right),
            },
            Expr::UnaryExpr { op, expr } => match op {
                UnaryOp::Floor => write!(f, "floor({})", expr),
                UnaryOp::Ceil => write!(f, "ceil({})", expr),
                UnaryOp::Sqrt => write!(f, "sqrt({})", expr),
                UnaryOp::Abs => write!(f, "abs({})", expr),
                UnaryOp::Round => write!(f, "round({})", expr),
            },
            Expr::AggregateExpr { op, expr } => match op {
                AggregateOp::Sum { .. } => write!(f, "sum({})", expr),
                AggregateOp::Count { .. } => write!(f, "count({})", expr),
                AggregateOp::Min { .. } => write!(f, "min({})", expr),
                AggregateOp::Max { .. } => write!(f, "max({})", expr),
                AggregateOp::Avg { .. } => write!(f, "avg({})", expr),
                _ => panic!("Invalid aggregate expression"),
            },
            Expr::Empty => write!(f, "empty"),
            Expr::Compiled {
                compiled_eval: _,
                expr,
            } => write!(f, "compiled<{}>", expr),
        }
    }
}

pub trait ExprEvaluable: Index<usize, Output = NoirType> {
    fn as_ptr(&self) -> *const NoirType;
}

impl ExprEvaluable for Vec<NoirType> {
    fn as_ptr(&self) -> *const NoirType {
        self.as_ptr()
    }
}

impl Expr {
    pub fn floor(self) -> Expr {
        unary_expr(UnaryOp::Floor, self)
    }

    pub fn ceil(self) -> Expr {
        unary_expr(UnaryOp::Ceil, self)
    }

    pub fn sqrt(self) -> Expr {
        unary_expr(UnaryOp::Sqrt, self)
    }

    pub fn abs(self) -> Expr {
        unary_expr(UnaryOp::Abs, self)
    }
    pub fn round(self) -> Expr {
        unary_expr(UnaryOp::Round, self)
    }

    pub fn modulo(self, rhs: impl Into<Expr>) -> Expr {
        binary_expr(self, BinaryOp::Mod, rhs.into())
    }

    pub fn eq(self, rhs: impl Into<Expr>) -> Expr {
        binary_expr(self, BinaryOp::Eq, rhs.into())
    }

    pub fn neq(self, rhs: impl Into<Expr>) -> Expr {
        binary_expr(self, BinaryOp::NotEq, rhs.into())
    }

    pub fn and(self, rhs: impl Into<Expr>) -> Expr {
        binary_expr(self, BinaryOp::And, rhs.into())
    }

    pub fn or(self, rhs: impl Into<Expr>) -> Expr {
        binary_expr(self, BinaryOp::Or, rhs.into())
    }

    pub fn xor(self, rhs: impl Into<Expr>) -> Expr {
        binary_expr(self, BinaryOp::Xor, rhs.into())
    }

    pub fn lt(self, rhs: impl Into<Expr>) -> Expr {
        binary_expr(self, BinaryOp::Lt, rhs.into())
    }

    pub fn lte(self, rhs: impl Into<Expr>) -> Expr {
        binary_expr(self, BinaryOp::LtEq, rhs.into())
    }

    pub fn gt(self, rhs: impl Into<Expr>) -> Expr {
        binary_expr(self, BinaryOp::Gt, rhs.into())
    }

    pub fn gte(self, rhs: impl Into<Expr>) -> Expr {
        binary_expr(self, BinaryOp::GtEq, rhs.into())
    }

    pub(crate) fn extract_dependencies(&self) -> Vec<usize> {
        let mut dependencies = vec![];
        let mut stack = vec![self];
        while let Some(current) = stack.pop() {
            match current {
                Expr::NthColumn(n) => {
                    dependencies.push(*n);
                }
                Expr::Literal(_) => (),
                Expr::BinaryExpr { left, op: _, right } => {
                    stack.push(left);
                    stack.push(right);
                }
                Expr::UnaryExpr { op: _, expr } => {
                    stack.push(expr);
                }
                Expr::AggregateExpr { op: _, expr } => {
                    stack.push(expr);
                }
                Expr::Empty => panic!("Empty expression has no dependencies"),
                Expr::Compiled { .. } => panic!("Compiled expression has no dependencies"),
            }
        }
        dependencies
    }

    pub fn depth(&self) -> usize {
        match self {
            Expr::NthColumn(_) => 1,
            Expr::Literal(_) => 1,
            Expr::BinaryExpr { left, op: _, right } => {
                let left = left.depth();
                let right = right.depth();
                left.max(right) + 1
            }
            Expr::UnaryExpr { op: _, expr } => expr.depth() + 1,
            Expr::AggregateExpr { op: _, expr } => expr.depth() + 1,
            Expr::Empty => panic!("Empty expression has no depth"),
            Expr::Compiled { .. } => panic!("Compiled expression has no depth"),
        }
    }

    pub fn compile(self, schema: &Schema, jit_compiler: &mut JitCompiler) -> Expr {
        if let Expr::AggregateExpr { op, expr } = self {
            return Expr::AggregateExpr {
                op,
                expr: Box::new(expr.compile(schema, jit_compiler)),
            };
        }

        let code = jit_compiler.compile(&self, schema).unwrap();
        Expr::Compiled {
            compiled_eval: unsafe { std::mem::transmute(code) },
            expr: Box::new(self),
        }
    }

    pub fn evaluate(&self, item: &impl ExprEvaluable) -> NoirType {
        match self {
            Expr::Literal(value) => value.clone(),
            Expr::NthColumn(n) => item[*n].clone(),
            Expr::BinaryExpr { left, op, right } => {
                let left = left.evaluate(item);
                let right = right.evaluate(item);
                match op {
                    BinaryOp::Plus => left + right,
                    BinaryOp::Minus => left - right,
                    BinaryOp::Multiply => left * right,
                    BinaryOp::Divide => left / right,
                    BinaryOp::Mod => left % right,
                    BinaryOp::Xor => left ^ right,
                    BinaryOp::Eq => NoirType::Bool(left == right),
                    BinaryOp::NotEq => NoirType::Bool(left != right),
                    BinaryOp::And => NoirType::Bool(left.into() && right.into()),
                    BinaryOp::Or => NoirType::Bool(left.into() || right.into()),
                    BinaryOp::Lt => NoirType::Bool(left < right),
                    BinaryOp::LtEq => NoirType::Bool(left <= right),
                    BinaryOp::Gt => NoirType::Bool(left > right),
                    BinaryOp::GtEq => NoirType::Bool(left >= right),
                }
            }
            Expr::UnaryExpr { op, expr } => {
                let data = expr.evaluate(item);
                match op {
                    UnaryOp::Floor => data.floor(),
                    UnaryOp::Ceil => data.ceil(),
                    UnaryOp::Sqrt => data.sqrt(),
                    UnaryOp::Abs => data.abs(),
                    UnaryOp::Round => data.round(),
                }
            }
            Expr::AggregateExpr { op: _, expr } => expr.evaluate(item),
            Expr::Empty => panic!("Empty expression"),
            Expr::Compiled {
                compiled_eval,
                expr: _,
            } => compiled_eval(item.as_ptr()),
        }
    }

    pub(crate) fn shift_left(&self, amount: usize) -> Expr {
        match self {
            Expr::NthColumn(n) => Expr::NthColumn(*n - amount),
            Expr::Literal(value) => Expr::Literal(value.clone()),
            Expr::BinaryExpr { left, op, right } => {
                let left = left.shift_left(amount);
                let right = right.shift_left(amount);
                Expr::BinaryExpr {
                    left: Box::new(left),
                    op: *op,
                    right: Box::new(right),
                }
            }
            Expr::UnaryExpr { op, expr } => {
                let expr = expr.shift_left(amount);
                Expr::UnaryExpr {
                    op: *op,
                    expr: Box::new(expr),
                }
            }
            Expr::AggregateExpr { op, expr } => {
                let expr = expr.shift_left(amount);
                Expr::AggregateExpr {
                    op: op.clone(),
                    expr: Box::new(expr),
                }
            }
            Expr::Empty => panic!("Empty expression"),
            Expr::Compiled { .. } => panic!("You can't modify a compiled expression"),
        }
    }

    pub(crate) fn is_aggregator(&self) -> bool {
        matches!(self, Expr::AggregateExpr { .. })
    }

    pub fn accumulator(self) -> AggregateOp {
        match self {
            Expr::AggregateExpr { op, .. } => op,
            _ => AggregateOp::Val(Val::new()),
        }
    }

    pub(crate) fn map_dependencies(&mut self, columns: &[Expr]) -> Expr {
        match self {
            Expr::NthColumn(n) => {
                let new_expr = columns
                    .get(*n)
                    .expect("Invalid column index during Expr::map_dependencies");
                match new_expr {
                    Expr::AggregateExpr { op: _, expr } => *expr.clone(),
                    Expr::Empty => panic!("Empty expression"),
                    Expr::Compiled { .. } => panic!("You can't modify a compiled expression"),
                    e => e.clone(),
                }
            }
            Expr::Literal(value) => Expr::Literal(value.clone()),
            Expr::BinaryExpr { left, op, right } => {
                let left = Box::new(left.map_dependencies(columns));
                let right = Box::new(right.map_dependencies(columns));
                Expr::BinaryExpr {
                    left,
                    op: *op,
                    right,
                }
            }
            Expr::UnaryExpr { op, expr } => {
                let expr = Box::new(expr.map_dependencies(columns));
                Expr::UnaryExpr { op: *op, expr }
            }
            Expr::AggregateExpr { .. } => {
                panic!("You can't map dependencies on an aggregate expression")
            }
            Expr::Empty => panic!("Empty expression"),
            Expr::Compiled { .. } => panic!("You can't modify a compiled expression"),
        }
    }
}

impl AggregateOp {
    pub(crate) fn accumulate(&mut self, value: NoirType) {
        match self {
            AggregateOp::Sum(ref mut state) => state.accumulate(value),
            AggregateOp::Count(ref mut state) => state.accumulate(value),
            AggregateOp::Min(ref mut state) => state.accumulate(value),
            AggregateOp::Max(ref mut state) => state.accumulate(value),
            AggregateOp::Avg(ref mut state) => state.accumulate(value),
            AggregateOp::Val(ref mut state) => state.accumulate(value),
        }
    }

    pub(crate) fn finalize(&self) -> NoirType {
        match self {
            AggregateOp::Sum(state) => state.finalize(),
            AggregateOp::Count(state) => state.finalize(),
            AggregateOp::Min(state) => state.finalize(),
            AggregateOp::Max(state) => state.finalize(),
            AggregateOp::Avg(state) => state.finalize(),
            AggregateOp::Val(state) => state.finalize(),
        }
    }

    #[allow(unused)]
    pub(crate) fn include(&mut self, other: AggregateOp) {
        match (self, other) {
            (AggregateOp::Sum(ref mut state), AggregateOp::Sum(other)) => state.include(other),
            (AggregateOp::Count(ref mut state), AggregateOp::Count(other)) => state.include(other),
            (AggregateOp::Min(ref mut state), AggregateOp::Min(other)) => state.include(other),
            (AggregateOp::Max(ref mut state), AggregateOp::Max(other)) => state.include(other),
            (AggregateOp::Avg(ref mut state), AggregateOp::Avg(other)) => state.include(other),
            (AggregateOp::Val(ref mut state), AggregateOp::Val(other)) => state.include(other),
            _ => panic!("Invalid aggregate operation"),
        }
    }
}

pub fn col(col: impl Into<ColumnIndex>) -> Expr {
    match col.into() {
        ColumnIndex::Nth(col) => Expr::NthColumn(col),
        ColumnIndex::Name(_) => panic!("Not implemented yet"),
    }
    // Expr::NthColumn(col.into())
}

pub fn binary_expr(lhs: Expr, op: BinaryOp, rhs: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(lhs),
        op,
        right: Box::new(rhs),
    }
}

pub fn unary_expr(op: UnaryOp, expr: Expr) -> Expr {
    Expr::UnaryExpr {
        op,
        expr: Box::new(expr),
    }
}

pub fn lit(value: impl Into<NoirType>) -> Expr {
    Expr::Literal(value.into())
}

pub fn i(value: i32) -> Expr {
    Expr::Literal(NoirType::Int(value))
}

pub fn f(value: f32) -> Expr {
    Expr::Literal(NoirType::Float(value))
}

pub fn b(value: bool) -> Expr {
    Expr::Literal(NoirType::Bool(value))
}

pub fn sum(expr: Expr) -> Expr {
    Expr::AggregateExpr {
        op: AggregateOp::Sum(Sum::new()),
        expr: Box::new(expr),
    }
}

pub fn count(expr: Expr) -> Expr {
    Expr::AggregateExpr {
        op: AggregateOp::Count(Count::new()),
        expr: Box::new(expr),
    }
}

pub fn min(expr: Expr) -> Expr {
    Expr::AggregateExpr {
        op: AggregateOp::Min(Min::new()),
        expr: Box::new(expr),
    }
}

pub fn max(expr: Expr) -> Expr {
    Expr::AggregateExpr {
        op: AggregateOp::Max(Max::new()),
        expr: Box::new(expr),
    }
}

pub fn avg(expr: Expr) -> Expr {
    Expr::AggregateExpr {
        op: AggregateOp::Avg(Avg::new()),
        expr: Box::new(expr),
    }
}
