use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::datatypes::NoirType;
use crate::dsl::expressions::*;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

use super::Data;

#[derive(Clone)]
pub struct FilterExpr<Op>
where
    Op: Operator + 'static,
    Op::Out: ExprEvaluable + Data,
{
    prev: Op,
    expression: Expr,
}

impl<Op> Display for FilterExpr<Op>
where
    Op: Operator + 'static,
    Op::Out: ExprEvaluable + Data,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> FilterExpr<{}>",
            self.prev,
            std::any::type_name::<Vec<NoirType>>()
        )
    }
}

impl<Op> FilterExpr<Op>
where
    Op: Operator + 'static,
    Op::Out: ExprEvaluable + Data,
{
    pub fn new(prev: Op, expression: Expr) -> Self {
        Self { prev, expression }
    }
}

impl<Op> Operator for FilterExpr<Op>
where
    Op: Operator + 'static,
    Op::Out: ExprEvaluable + Data,
{
    type Out = Op::Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<Op::Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item(ref item) | StreamElement::Timestamped(ref item, _)
                    if bool::from(!self.expression.evaluate(item)) => {}
                element => return element,
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Vec<NoirType>, _>("Filter"))
    }
}
