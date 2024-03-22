use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::data_type::noir_data::NoirData;
use crate::operator::{Operator, StreamElement};
use crate::optimization::dsl::expressions::{Expr, ExprEvaluable};
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
            std::any::type_name::<NoirData>()
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
            .add_operator(OperatorStructure::new::<NoirData, _>("Filter"))
    }
}

#[cfg(test)]
pub mod test {
    use itertools::Itertools;
    use rand::{rngs::ThreadRng, Rng};

    use crate::data_type::noir_data::NoirData;
    use crate::data_type::noir_type::NoirType;
    use crate::{
        operator::{Operator, StreamElement},
        optimization::dsl::expressions::*,
        test::FakeOperator,
    };

    use super::FilterExpr;

    fn random_row(rng: &mut ThreadRng) -> NoirData {
        let col = 5;
        let mut row = Vec::with_capacity(col);
        for _ in 0..col {
            row.push(NoirType::Int32(rng.gen()))
        }
        NoirData::Row(row)
    }

    fn test_predicate(
        data: Vec<NoirData>,
        predicate_closure: impl Fn(&NoirData) -> bool,
        predicte_expression: Expr,
    ) {
        let expected = data
            .clone()
            .into_iter()
            .filter(predicate_closure)
            .collect_vec();

        let fake_operator = FakeOperator::new(data.into_iter());
        let mut filter = FilterExpr::new(fake_operator, predicte_expression);

        for item in expected {
            assert_eq!(filter.next(), StreamElement::Item(item));
        }
        assert_eq!(filter.next(), StreamElement::Terminate);
    }

    #[test]
    fn test_filter_stream() {
        let mut rng = rand::thread_rng();
        test_predicate(
            (0..100).map(|_| random_row(&mut rng)).collect_vec(),
            |row| row[1] % NoirType::Int32(10) == NoirType::Int32(0),
            col(1).modulo(i(10)).eq(i(0)),
        );
        test_predicate(
            (0..100).map(|_| random_row(&mut rng)).collect_vec(),
            |row| row[1] == row[0],
            col(1).eq(col(0)),
        );
        test_predicate(
            (0..100).map(|_| random_row(&mut rng)).collect_vec(),
            |row| row[1] != row[0],
            col(1).neq(col(0)),
        );
        test_predicate(
            (0..100).map(|_| random_row(&mut rng)).collect_vec(),
            |row| row[1] == row[0] + NoirType::Int32(1),
            col(1).eq(col(0) + i(1)),
        );
    }

    // #[test]
    // fn test_filter_stream_with_keyed() {
    //     let mut rng = rand::thread_rng();
    //     test_predicate(
    //         (0..100).map(|_| random_keyed_row(&mut rng)).collect_vec(),
    //         |data| data[1] % NoirType::Int32(10) == NoirType::Int32(0),
    //         col(1).modulo(i(10)).eq(i(0)),
    //     );
    //     test_predicate(
    //         (0..100).map(|_| random_keyed_row(&mut rng)).collect_vec(),
    //         |data| data[4] == data[0],
    //         col(4).eq(col(0)),
    //     );
    //     test_predicate(
    //         (0..100).map(|_| random_keyed_row(&mut rng)).collect_vec(),
    //         |data| data[4] != data[0],
    //         col(4).neq(col(0)),
    //     );
    // }
}
