use core::panic;
use std::fmt::{Debug, Display};
use std::ops::Index;

use crate::data_type::noir_type::NoirType;
use crate::data_type::schema::Schema;
use crate::data_type::stream_item::StreamItem;
use crate::optimization::arena::Arena;

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

#[derive(Clone, PartialEq, Eq, Debug, Copy, Hash)]
pub enum AggregateOp {
    Sum,
    Count,
    Min,
    Max,
    Avg,
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
    Empty,
}

pub struct CompiledExpr {
    compiled_expression: Option<extern "C" fn(*const NoirType, *mut NoirType)>,
}

impl CompiledExpr {
    pub fn compile(expr: Expr, schema: Schema) -> Self {
        let mut jit = JitCompiler::default();
        let code_ptr = jit.compile(expr, schema).unwrap();
        let compiled_expr: extern "C" fn(*const NoirType, *mut NoirType) =
            unsafe { std::mem::transmute(code_ptr) };
        Self {
            compiled_expression: Some(compiled_expr),
        }
    }

    pub fn evaluate(&self, item: &[NoirType]) -> NoirType {
        let mut result = NoirType::Int32(0);
        if let Some(compiled_expr) = self.compiled_expression {
            compiled_expr(item.as_ptr(), &mut result);
            result
        } else {
            panic!("Compiled expression is not available");
        }
    }
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
            },
            Expr::Empty => write!(f, "empty"),
        }
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
                Expr::Empty => panic!("Empty expression"),
            }
        }
        dependencies
    }

    pub fn evaluate<T: Index<usize, Output = NoirType>>(&self, item: &T) -> NoirType {
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
            Expr::AggregateExpr { op, expr } => {
                let data = expr.evaluate(item);
                match op {
                    AggregateOp::Sum => data,
                    AggregateOp::Count => NoirType::Int32(1),
                    AggregateOp::Min => data,
                    AggregateOp::Max => data,
                    AggregateOp::Avg => todo!(),
                }
            }
            Expr::Empty => panic!("Empty expression"),
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
        }
    }

    pub(crate) fn is_aggregator(&self) -> bool {
        matches!(self, Expr::AggregateExpr { .. })
    }

    pub(crate) fn accumulate(&self, a: NoirType, b: NoirType) -> NoirType {
        match self {
            Expr::AggregateExpr { op, .. } => match op {
                AggregateOp::Sum => a + b,
                AggregateOp::Count => a + b,
                AggregateOp::Min => {
                    if a > b {
                        b
                    } else {
                        a
                    }
                }
                AggregateOp::Max => {
                    if a < b {
                        b
                    } else {
                        a
                    }
                }
                AggregateOp::Avg => todo!(),
            },
            _ => {
                assert_eq!(a, b);
                a
            }
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
        ArenaExpr::AggregateExpr { op, expr } => {
            let data = evaluate_arena_expr(expr_arena.get(*expr).unwrap(), item, expr_arena);
            match op {
                AggregateOp::Sum => data,
                AggregateOp::Count => NoirType::Int32(1),
                AggregateOp::Min => data,
                AggregateOp::Max => data,
                AggregateOp::Avg => todo!(),
            }
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
        op: AggregateOp::Sum,
        expr: Box::new(expr),
    }
}

pub fn count(expr: Expr) -> Expr {
    Expr::AggregateExpr {
        op: AggregateOp::Count,
        expr: Box::new(expr),
    }
}

pub fn min(expr: Expr) -> Expr {
    Expr::AggregateExpr {
        op: AggregateOp::Min,
        expr: Box::new(expr),
    }
}

pub fn max(expr: Expr) -> Expr {
    Expr::AggregateExpr {
        op: AggregateOp::Max,
        expr: Box::new(expr),
    }
}

pub fn avg(expr: Expr) -> Expr {
    Expr::AggregateExpr {
        op: AggregateOp::Avg,
        expr: Box::new(expr),
    }
}

#[cfg(test)]
pub mod test {
    use std::vec;

    use crate::data_type::noir_data::NoirData;
    use crate::data_type::noir_type::NoirType;

    use super::*;

    #[test]
    fn test_expr() {
        let data: StreamItem = NoirData::Row(vec![
            NoirType::Int32(1),
            NoirType::Int32(2),
            NoirType::Int32(3),
        ])
        .into();

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
