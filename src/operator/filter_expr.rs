use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::data_type::NoirData;
use crate::operator::{Operator, StreamElement};
use crate::optimization::dsl::expressions::Expr;
use crate::scheduler::ExecutionMetadata;

#[derive(Clone)]
pub struct FilterExpr<PreviousOperator>
where
    PreviousOperator: Operator<Out = NoirData> + 'static,
{
    prev: PreviousOperator,
    expression: Expr,
}

impl<PreviousOperator> Display for FilterExpr<PreviousOperator>
where
    PreviousOperator: Operator<Out = NoirData> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> FilterExpr<{}>",
            self.prev,
            std::any::type_name::<NoirData>()
        )
    }
}

impl<PreviousOperator> FilterExpr<PreviousOperator>
where
    PreviousOperator: Operator<Out = NoirData> + 'static,
{
    pub fn new(prev: PreviousOperator, expression: Expr) -> Self {
        Self { prev, expression }
    }
}

impl<PreviousOperator> Operator for FilterExpr<PreviousOperator>
where
    PreviousOperator: Operator<Out = NoirData> + 'static,
{
    type Out = NoirData;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<NoirData> {
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
            .add_operator(OperatorStructure::new::<NoirData, _>("Filter"))
    }
}
