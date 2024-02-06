use core::panic;
use std::fmt::{Debug, Display};

use crate::data_type::noir_type::NoirType;
use crate::data_type::stream_item::StreamItem;

#[derive(Clone, PartialEq, Eq, Debug, Copy, Hash)]
pub enum ExprOp {
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
        op: ExprOp,
        right: ExprRef,
    },
    UnaryExpr {
        op: ExprOp,
        expr: ExprRef,
    },
    AggregateExpr {
        op: AggregateOp,
        expr: ExprRef,
    },
    Empty,
}

impl Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::NthColumn(n) => write!(f, "col({})", n),
            Expr::Literal(value) => write!(f, "{}", value),
            Expr::BinaryExpr { left, op, right } => match op {
                ExprOp::Plus => write!(f, "({} + {})", left, right),
                ExprOp::Minus => write!(f, "({} - {})", left, right),
                ExprOp::Multiply => write!(f, "({} * {})", left, right),
                ExprOp::Divide => write!(f, "({} / {})", left, right),
                ExprOp::Eq => write!(f, "({} == {})", left, right),
                ExprOp::And => write!(f, "({} && {})", left, right),
                ExprOp::NotEq => write!(f, "({} != {})", left, right),
                ExprOp::Lt => write!(f, "({} < {})", left, right),
                ExprOp::LtEq => write!(f, "({} <= {})", left, right),
                ExprOp::Gt => write!(f, "({} > {})", left, right),
                ExprOp::GtEq => write!(f, "({} >= {})", left, right),
                ExprOp::Or => write!(f, "({} || {})", left, right),
                ExprOp::Xor => write!(f, "({} ^ {})", left, right),
                ExprOp::Mod => write!(f, "({} % {})", left, right),
                _ => panic!("Unsupported operator"),
            },
            Expr::UnaryExpr { op, expr } => match op {
                ExprOp::Floor => write!(f, "floor({})", expr),
                ExprOp::Ceil => write!(f, "ceil({})", expr),
                ExprOp::Sqrt => write!(f, "sqrt({})", expr),
                ExprOp::Abs => write!(f, "abs({})", expr),
                ExprOp::Round => write!(f, "round({})", expr),
                _ => panic!("Unsupported operator"),
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
        unary_expr(ExprOp::Floor, self)
    }

    pub fn ceil(self) -> Expr {
        unary_expr(ExprOp::Ceil, self)
    }

    pub fn sqrt(self) -> Expr {
        unary_expr(ExprOp::Sqrt, self)
    }

    pub fn abs(self) -> Expr {
        unary_expr(ExprOp::Abs, self)
    }
    pub fn round(self) -> Expr {
        unary_expr(ExprOp::Round, self)
    }

    pub fn modulo(self, rhs: Expr) -> Expr {
        binary_expr(self, ExprOp::Mod, rhs)
    }

    pub fn eq(self, rhs: Expr) -> Expr {
        binary_expr(self, ExprOp::Eq, rhs)
    }

    pub fn neq(self, rhs: Expr) -> Expr {
        binary_expr(self, ExprOp::NotEq, rhs)
    }

    pub fn and(self, rhs: Expr) -> Expr {
        binary_expr(self, ExprOp::And, rhs)
    }

    pub fn or(self, rhs: Expr) -> Expr {
        binary_expr(self, ExprOp::Or, rhs)
    }

    pub fn xor(self, rhs: Expr) -> Expr {
        binary_expr(self, ExprOp::Xor, rhs)
    }

    pub fn lt(self, rhs: Expr) -> Expr {
        binary_expr(self, ExprOp::Lt, rhs)
    }

    pub fn lte(self, rhs: Expr) -> Expr {
        binary_expr(self, ExprOp::LtEq, rhs)
    }

    pub fn gt(self, rhs: Expr) -> Expr {
        binary_expr(self, ExprOp::Gt, rhs)
    }

    pub fn gte(self, rhs: Expr) -> Expr {
        binary_expr(self, ExprOp::GtEq, rhs)
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

    pub fn evaluate(&self, item: &StreamItem) -> NoirType {
        match self {
            Expr::Literal(value) => *value,
            Expr::NthColumn(n) => item[*n],
            Expr::BinaryExpr { left, op, right } => {
                let left = left.evaluate(item);
                let right = right.evaluate(item);
                match op {
                    ExprOp::Plus => left + right,
                    ExprOp::Minus => left - right,
                    ExprOp::Multiply => left * right,
                    ExprOp::Divide => left / right,
                    ExprOp::Mod => left % right,
                    ExprOp::Xor => left ^ right,
                    ExprOp::Eq => NoirType::Bool(left == right),
                    ExprOp::NotEq => NoirType::Bool(left != right),
                    ExprOp::And => NoirType::Bool(left.into() && right.into()),
                    ExprOp::Or => NoirType::Bool(left.into() || right.into()),
                    ExprOp::Lt => NoirType::Bool(left < right),
                    ExprOp::LtEq => NoirType::Bool(left <= right),
                    ExprOp::Gt => NoirType::Bool(left > right),
                    ExprOp::GtEq => NoirType::Bool(left >= right),
                    _ => panic!("Unsupported operator"),
                }
            }
            Expr::UnaryExpr { op, expr } => {
                let data = expr.evaluate(item);
                match op {
                    ExprOp::Floor => data.floor(),
                    ExprOp::Ceil => data.ceil(),
                    ExprOp::Sqrt => data.sqrt(),
                    ExprOp::Abs => data.abs(),
                    ExprOp::Round => data.round(),
                    _ => panic!("Unsupported operator"),
                }
            }
            // Expr::AggregateExpr {..} => panic!("Aggregates should not be evaluated"),
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
}

pub fn col(n: usize) -> Expr {
    Expr::NthColumn(n)
}

pub fn binary_expr(lhs: Expr, op: ExprOp, rhs: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(lhs),
        op,
        right: Box::new(rhs),
    }
}

pub fn unary_expr(op: ExprOp, expr: Expr) -> Expr {
    Expr::UnaryExpr {
        op,
        expr: Box::new(expr),
    }
}

pub fn lit(value: NoirType) -> Expr {
    Expr::Literal(value)
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
