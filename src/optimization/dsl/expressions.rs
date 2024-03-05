use core::panic;
use std::fmt::{Debug, Display};
use std::ops::Index;

use serde::{Deserialize, Serialize};

use crate::data_type::noir_type::NoirType;
use crate::data_type::schema::Schema;
use crate::data_type::stream_item::StreamItem;
use crate::optimization::arena::Arena;

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

#[derive(Clone, PartialEq, Eq, Debug, Copy, Hash, Serialize, Deserialize)]
pub enum AggregateOp {
    Val(Val),
    Sum(Sum),
    Count(Count),
    Min(Min),
    Max(Max),
    Avg(Avg),
}

pub type ExprRef = Box<Expr>;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Expr {
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
            Expr::Literal(value) => *value,
            Expr::NthColumn(n) => item[*n],
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
            Expr::Literal(value) => Expr::Literal(*value),
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
                    op: *op,
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

    pub fn into_arena(self, expr_arena: &mut Arena<ArenaExpr>) -> ArenaExpr {
        match self {
            Expr::NthColumn(n) => ArenaExpr::NthColumn(n),
            Expr::Literal(value) => ArenaExpr::Literal(value),
            Expr::BinaryExpr { left, op, right } => {
                let arena_left = left.into_arena(expr_arena);
                let arena_right = right.into_arena(expr_arena);
                let new_left = expr_arena.alloc(arena_left);
                let new_right = expr_arena.alloc(arena_right);
                ArenaExpr::BinaryExpr {
                    left: new_left,
                    op,
                    right: new_right,
                }
            }
            Expr::UnaryExpr { op, expr } => {
                let expr = expr.into_arena(expr_arena);
                let new_expr = expr_arena.alloc(expr);
                ArenaExpr::UnaryExpr { op, expr: new_expr }
            }
            Expr::AggregateExpr { op, expr } => {
                let expr = expr.into_arena(expr_arena);
                let new_expr = expr_arena.alloc(expr);
                ArenaExpr::AggregateExpr { op, expr: new_expr }
            }
            Expr::Empty => ArenaExpr::Empty,
            Expr::Compiled { .. } => {
                panic!("You can't convert a compiled expression to an arena expression")
            }
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
            (AggregateOp::Sum(state), AggregateOp::Sum(other)) => state.include(other),
            (AggregateOp::Count(state), AggregateOp::Count(other)) => state.include(other),
            (AggregateOp::Min(state), AggregateOp::Min(other)) => state.include(other),
            (AggregateOp::Max(state), AggregateOp::Max(other)) => state.include(other),
            (AggregateOp::Avg(state), AggregateOp::Avg(other)) => state.include(other),
            (AggregateOp::Val(state), AggregateOp::Val(other)) => state.include(other),
            _ => panic!("Invalid aggregate operation"),
        }
    }
}

pub fn evaluate_arena_expr(
    expr: &ArenaExpr,
    item: &StreamItem,
    expr_arena: &Arena<ArenaExpr>,
) -> NoirType {
    match expr {
        ArenaExpr::NthColumn(n) => item[*n],
        ArenaExpr::Literal(value) => *value,
        ArenaExpr::BinaryExpr { left, op, right } => {
            let left = evaluate_arena_expr(expr_arena.get(*left).unwrap(), item, expr_arena);
            let right = evaluate_arena_expr(expr_arena.get(*right).unwrap(), item, expr_arena);
            match op {
                BinaryOp::Plus => left + right,
                BinaryOp::Minus => left - right,
                BinaryOp::Multiply => left * right,
                BinaryOp::Divide => left / right,
                BinaryOp::Mod => left % right,
                BinaryOp::And => left & right,
                BinaryOp::Or => left | right,
                BinaryOp::Xor => left ^ right,
                BinaryOp::Eq => NoirType::Bool(left == right),
                BinaryOp::NotEq => NoirType::Bool(left != right),
                BinaryOp::Lt => NoirType::Bool(left < right),
                BinaryOp::LtEq => NoirType::Bool(left <= right),
                BinaryOp::Gt => NoirType::Bool(left > right),
                BinaryOp::GtEq => NoirType::Bool(left >= right),
            }
        }
        ArenaExpr::UnaryExpr { op, expr } => {
            let data = evaluate_arena_expr(expr_arena.get(*expr).unwrap(), item, expr_arena);
            match op {
                UnaryOp::Floor => data.floor(),
                UnaryOp::Ceil => data.ceil(),
                UnaryOp::Sqrt => data.sqrt(),
                UnaryOp::Abs => data.abs(),
                UnaryOp::Round => data.round(),
            }
        }
        ArenaExpr::AggregateExpr { op: _, expr } => {
            evaluate_arena_expr(expr_arena.get(*expr).unwrap(), item, expr_arena)
        }
        ArenaExpr::Empty => panic!("Empty expression"),
    }
}

pub fn col(n: usize) -> Expr {
    Expr::NthColumn(n)
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
    Expr::Literal(NoirType::Int32(value))
}

pub fn f(value: f32) -> Expr {
    Expr::Literal(NoirType::Float32(value))
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

#[cfg(test)]
pub mod test {
    use std::vec;

    use crate::data_type::noir_type::NoirType;

    use super::*;

    #[test]
    fn test_expr() {
        let data = vec![NoirType::Int32(1), NoirType::Int32(2), NoirType::Int32(3)];

        let expr = col(0) + i(1);
        assert_eq!(expr.evaluate(&data), NoirType::Int32(2));

        let expr = col(0) - i(1);
        assert_eq!(expr.evaluate(&data), NoirType::Int32(0));

        let expr = col(0) * i(2);
        assert_eq!(expr.evaluate(&data), NoirType::Int32(2));

        let expr = col(0) / i(2);
        assert_eq!(expr.evaluate(&data), NoirType::Int32(0));

        let expr = col(0) % i(2);
        assert_eq!(expr.evaluate(&data), NoirType::Int32(1));

        let expr = col(0) ^ i(2);
        assert_eq!(expr.evaluate(&data), NoirType::Int32(3));

        let expr = col(0).eq(i(1));
        assert_eq!(expr.evaluate(&data), NoirType::Bool(true));

        let expr = col(0).neq(i(1));
        assert_eq!(expr.evaluate(&data), NoirType::Bool(false));

        let expr = col(0).eq(i(0));
        assert_eq!(expr.evaluate(&data), NoirType::Bool(false));

        let expr = col(0).neq(i(0));
        assert_eq!(expr.evaluate(&data), NoirType::Bool(true));
    }
}
