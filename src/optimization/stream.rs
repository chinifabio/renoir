use std::path::PathBuf;

use crate::{
    block::NextStrategy,
    data_type::{NoirData, NoirType, Schema},
    operator::{
        end::End, filter_expr::FilterExpr, key_by::KeyBy, sink::StreamOutput, Operator,
        SimpleStartReceiver, Start,
    },
    optimization::dsl::expressions::Expr,
    stream::OptStream,
    KeyedStream, Stream,
};

use super::{
    logical_plan::{JoinType, LogicPlan},
    physical_plan::{to_stream, CsvRow},
};

impl<Op> Stream<Op>
where
    Op: Operator + 'static,
    Op::Out: CsvRow,
{
    pub fn filter_expr(self, expr: Expr) -> Stream<FilterExpr<Op>> {
        self.add_operator(|prev| FilterExpr::new(prev, expr))
    }

    pub fn group_by_expr<T2: CsvRow>(
        self,
        keyer: Expr,
    ) -> KeyedStream<
        KeyBy<NoirType, impl Fn(&Op::Out) -> NoirType + Clone, Start<SimpleStartReceiver<Op::Out>>>,
    > {
        let keyer_closure = move |item: &Op::Out| keyer.evaluate(item);
        let next_strategy = NextStrategy::group_by(keyer_closure.clone());
        let stream = self
            .split_block(End::new, next_strategy)
            .add_operator(|prev| KeyBy::new(prev, keyer_closure));
        KeyedStream(stream)
    }
}

impl<Op> KeyedStream<Op>
where
    Op: Operator + 'static,
    Op::Out: CsvRow,
{
    pub fn filter_expr(self, expr: Expr) -> KeyedStream<FilterExpr<Op>> {
        self.add_operator(|prev| FilterExpr::new(prev, expr))
    }
}

impl OptStream {
    pub fn collect_vec(self) -> StreamOutput<Vec<NoirData>> {
        let optimized = self.logic_plan.collect_vec().optimize();
        info!("Optimized plan: {}", optimized);
        to_stream(optimized, self.inner).into_output()
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

    pub fn select<E: AsRef<[Expr]>>(self, exprs: E) -> Self {
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

    pub fn join<E: AsRef<[Expr]>>(self, other: OptStream, left_on: E, right_on: E) -> OptStream {
        OptStream {
            inner: self.inner,
            logic_plan: self
                .logic_plan
                .join(other.logic_plan, left_on, right_on, JoinType::Inner),
        }
    }

    pub fn left_join<E: AsRef<[Expr]>>(
        self,
        other: OptStream,
        left_on: E,
        right_on: E,
    ) -> OptStream {
        OptStream {
            inner: self.inner,
            logic_plan: self
                .logic_plan
                .join(other.logic_plan, left_on, right_on, JoinType::Left),
        }
    }

    pub fn full_join<E: AsRef<[Expr]>>(
        self,
        other: OptStream,
        left_on: E,
        right_on: E,
    ) -> OptStream {
        OptStream {
            inner: self.inner,
            logic_plan: self
                .logic_plan
                .join(other.logic_plan, left_on, right_on, JoinType::Outer),
        }
    }

    pub fn with_schema(self, schema: Schema) -> Self {
        let new_plan = match self.logic_plan {
            LogicPlan::TableScan {
                path,
                predicate,
                projections,
                ..
            } => LogicPlan::TableScan {
                path,
                predicate,
                projections,
                schema: Some(schema),
            },
            _ => panic!("Cannot set schema on non TableScan plan"),
        };
        Self {
            inner: self.inner,
            logic_plan: new_plan,
        }
    }

    pub fn infer_schema(self) -> Self {
        let new_plan = match self.logic_plan {
            LogicPlan::TableScan {
                path,
                predicate,
                projections,
                ..
            } => LogicPlan::TableScan {
                path: path.clone(),
                predicate,
                projections,
                schema: Some(Schema::infer_from_file(path)),
            },
            _ => panic!("Cannot infer schema on non TableScan plan"),
        };
        Self {
            inner: self.inner.clone(),
            logic_plan: new_plan,
        }
    }
}

impl crate::StreamEnvironment {
    pub fn stream_csv_optimized(&mut self, path: impl Into<PathBuf>) -> OptStream {
        let path = path.into();
        OptStream {
            inner: self.inner.clone(),
            logic_plan: LogicPlan::TableScan {
                path: path.clone(),
                predicate: None,
                projections: None,
                schema: None,
            },
        }
    }
}
