use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::data_type::{NoirData, NoirType};
use crate::operator::{Operator, StreamElement};
use crate::optimization::dsl::expressions::Expr;
use crate::scheduler::ExecutionMetadata;

#[derive(Clone)]
pub struct KeyedFilterExpr<PreviousOperator>
where
    PreviousOperator: Operator<Out = (NoirType, NoirData)> + 'static,
{
    prev: PreviousOperator,
    expression: Expr,
}

impl<PreviousOperator> Display for KeyedFilterExpr<PreviousOperator>
where
    PreviousOperator: Operator<Out = (NoirType, NoirData)> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> KeyedFilterExpr<{}>",
            self.prev,
            std::any::type_name::<NoirData>()
        )
    }
}

impl<PreviousOperator> KeyedFilterExpr<PreviousOperator>
where
    PreviousOperator: Operator<Out = (NoirType, NoirData)> + 'static,
{
    pub fn new(prev: PreviousOperator, expression: Expr) -> Self {
        Self { prev, expression }
    }
}

impl<PreviousOperator> Operator for KeyedFilterExpr<PreviousOperator>
where
    PreviousOperator: Operator<Out = (NoirType, NoirData)> + 'static,
{
    type Out = (NoirType, NoirData);

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<(NoirType, NoirData)> {
        loop {
            match self.prev.next() {
                StreamElement::Item(ref item) | StreamElement::Timestamped(ref item, _)
                    if bool::from(!self.expression.evaluate(&item.1)) => {}
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
