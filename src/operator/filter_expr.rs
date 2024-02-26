use std::fmt::Display;

use crate::block::{BlockStructure, OperatorStructure};
use crate::data_type::noir_data::NoirData;
use crate::data_type::stream_item::StreamItem;
use crate::operator::{Operator, StreamElement};
use crate::optimization::dsl::expressions::Expr;
use crate::scheduler::ExecutionMetadata;

#[derive(Clone)]
pub struct FilterExpr<PreviousOperator>
where
    PreviousOperator: Operator<Out = StreamItem> + 'static,
{
    prev: PreviousOperator,
    expression: Expr,
}

impl<PreviousOperator> Display for FilterExpr<PreviousOperator>
where
    PreviousOperator: Operator<Out = StreamItem> + 'static,
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
    PreviousOperator: Operator<Out = StreamItem> + 'static,
{
    pub fn new(prev: PreviousOperator, expression: Expr) -> Self {
        Self { prev, expression }
    }
}

impl<PreviousOperator> Operator for FilterExpr<PreviousOperator>
where
    PreviousOperator: Operator<Out = StreamItem> + 'static,
{
    type Out = PreviousOperator::Out;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    #[inline]
    fn next(&mut self) -> StreamElement<PreviousOperator::Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item(ref item) | StreamElement::Timestamped(ref item, _)
                    if bool::from(!self.expression.evaluate(item.get_value())) => {}
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
    use crate::data_type::stream_item::StreamItem;
    use crate::{
        operator::{Operator, StreamElement},
        optimization::dsl::expressions::*,
        test::FakeOperator,
    };

    use super::FilterExpr;

    fn random_row(rng: &mut ThreadRng) -> StreamItem {
        let col = 5;
        let mut row = Vec::with_capacity(col);
        for _ in 0..col {
            row.push(NoirType::Int32(rng.gen()))
        }
        StreamItem::from(NoirData::Row(row))
    }

    fn random_keyed_row(rng: &mut ThreadRng) -> StreamItem {
        StreamItem::from(NoirData::from(random_row(rng)))
    }

    fn test_predicate(
        data: Vec<StreamItem>,
        predicate_closure: impl Fn(&StreamItem) -> bool,
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

    #[test]
    fn test_filter_stream_with_keyed() {
        let mut rng = rand::thread_rng();
        test_predicate(
            (0..100).map(|_| random_keyed_row(&mut rng)).collect_vec(),
            |data| data[1] % NoirType::Int32(10) == NoirType::Int32(0),
            col(1).modulo(i(10)).eq(i(0)),
        );
        test_predicate(
            (0..100).map(|_| random_keyed_row(&mut rng)).collect_vec(),
            |data| data[4] == data[0],
            col(4).eq(col(0)),
        );
        test_predicate(
            (0..100).map(|_| random_keyed_row(&mut rng)).collect_vec(),
            |data| data[4] != data[0],
            col(4).neq(col(0)),
        );
    }
}
