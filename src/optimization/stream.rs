use std::path::PathBuf;

use crate::{
    block::NextStrategy,
    box_op::BoxedOperator,
    data_type::{NoirData, NoirType},
    operator::{
        end::End, filter_expr::FilterExpr, key_by::KeyBy, keyed_filter_expr::KeyedFilterExpr,
        sink::StreamOutput, Operator,
    },
    optimization::dsl::expressions::Expr,
    stream::OptStream,
    KeyedStream, Stream,
};

use super::logical_plan::LogicPlan;

impl<Op> Stream<Op>
where
    Op: Operator<Out = NoirData> + 'static,
{
    pub fn filter_expr(self, expr: Expr) -> Stream<impl Operator<Out = Op::Out>> {
        self.add_operator(|prev| FilterExpr::new(prev, expr))
    }

    pub fn group_by_expr(self, keyer: Expr) -> KeyedStream<BoxedOperator<(NoirType, NoirData)>> {
        let keyer_closure = move |item: &NoirData| keyer.evaluate(item);
        let next_strategy = NextStrategy::group_by(keyer_closure.clone());
        let stream = self
            .split_block(End::new, next_strategy)
            .add_operator(|prev| KeyBy::new(prev, keyer_closure));
        KeyedStream(stream).into_box()
    }
}

impl<Op> KeyedStream<Op>
where
    Op: Operator<Out = (NoirType, NoirData)> + 'static,
{
    pub fn filter_expr(self, expr: Expr) -> KeyedStream<BoxedOperator<(NoirType, NoirData)>> {
        self.add_operator(|prev| KeyedFilterExpr::new(prev, expr))
            .into_box()
    }
}

impl OptStream {
    pub fn collect_vec(self) -> StreamOutput<Vec<NoirData>> {
        self.logic_plan
            .collect_vec()
            .optimize()
            .to_stream(self.inner)
            .into_output()
    }

    pub fn filter(self, predicate: Expr) -> Self {
        OptStream {
            inner: self.inner,
            logic_plan: self.logic_plan.filter(predicate),
        }
    }

    pub fn shuffle(self) -> Self {
        OptStream {
            inner: self.inner,
            logic_plan: self.logic_plan.shuffle(),
        }
    }

    pub fn group_by(self, key: Expr) -> Self {
        OptStream {
            inner: self.inner,
            logic_plan: self.logic_plan.group_by(key),
        }
    }

    pub fn select(self, exprs: &[Expr]) -> Self {
        OptStream {
            inner: self.inner,
            logic_plan: self.logic_plan.select(exprs),
        }
    }

    pub fn drop_key(self) -> Self {
        OptStream {
            inner: self.inner,
            logic_plan: self.logic_plan.drop_key(),
        }
    }

    pub fn drop(self, cols: Vec<usize>) -> Self {
        OptStream {
            inner: self.inner,
            logic_plan: self.logic_plan.drop(cols),
        }
    }
}

impl crate::StreamEnvironment {
    pub fn optimized_csv_stream(&mut self, path: impl Into<PathBuf>) -> OptStream {
        OptStream {
            inner: self.inner.clone(),
            logic_plan: LogicPlan::TableScan {
                path: path.into(),
                predicate: None,
                projections: None,
            },
        }
    }
}
