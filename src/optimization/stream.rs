use std::path::PathBuf;

use crate::data_type::schema::Schema;
use crate::data_type::stream_item::StreamItem;
use crate::{
    box_op::BoxedOperator,
    operator::{filter_expr::FilterExpr, sink::StreamOutput, Operator},
    optimization::dsl::expressions::Expr,
    stream::OptStream,
    Stream,
};

use super::optimizer::OptimizationOptions;
use super::{
    logical_plan::{JoinType, LogicPlan},
    physical_plan::to_stream,
};

impl<Op> Stream<Op>
where
    Op: Operator<Out = StreamItem> + 'static,
{
    pub fn filter_expr(self, expr: Expr) -> Stream<FilterExpr<Op>> {
        self.add_operator(|prev| FilterExpr::new(prev, expr))
    }

    pub fn group_by_expr(self, keys: Vec<Expr>) -> Stream<BoxedOperator<StreamItem>> {
        self.group_by(move |item: &StreamItem| {
            keys.iter().map(|k| k.evaluate(item.get_value())).collect()
        })
        .0
        .map(|(k, v)| v.absorb_key(k))
        .into_box()
    }
}

impl OptStream {
    pub fn collect_vec(self) -> StreamOutput<Vec<StreamItem>> {
        let optimized = self.logic_plan.collect_vec().optimize(self.optimizations);
        info!("Optimized plan: {}", optimized);
        to_stream(optimized, self.inner).into_output()
    }

    pub fn filter(self, predicate: Expr) -> Self {
        OptStream {
            inner: self.inner,
            logic_plan: self.logic_plan.filter(predicate),
            optimizations: self.optimizations,
        }
    }

    pub fn shuffle(self) -> Self {
        OptStream {
            inner: self.inner,
            logic_plan: self.logic_plan.shuffle(),
            optimizations: self.optimizations,
        }
    }

    pub fn group_by<E: AsRef<[Expr]>>(self, key: E) -> Self {
        OptStream {
            inner: self.inner,
            logic_plan: self.logic_plan.group_by(key),
            optimizations: self.optimizations,
        }
    }

    pub fn select<E: AsRef<[Expr]>>(self, exprs: E) -> Self {
        OptStream {
            inner: self.inner,
            logic_plan: self.logic_plan.select(exprs),
            optimizations: self.optimizations,
        }
    }

    pub fn drop_key(self) -> Self {
        OptStream {
            inner: self.inner,
            logic_plan: self.logic_plan.drop_key(),
            optimizations: self.optimizations,
        }
    }

    pub fn drop(self, cols: Vec<usize>) -> Self {
        OptStream {
            inner: self.inner,
            logic_plan: self.logic_plan.drop(cols),
            optimizations: self.optimizations,
        }
    }

    pub fn join<E: AsRef<[Expr]>>(self, other: OptStream, left_on: E, right_on: E) -> OptStream {
        OptStream {
            optimizations: self.optimizations,
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
            optimizations: self.optimizations,
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
            optimizations: self.optimizations,
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
            optimizations: self.optimizations,
        }
    }

    pub fn with_optimizations(self, optimizations: OptimizationOptions) -> Self {
        Self {
            inner: self.inner,
            logic_plan: self.logic_plan,
            optimizations,
        }
    }

    pub fn with_compiled_expressions(self, compiled: bool) -> Self {
        Self {
            inner: self.inner,
            logic_plan: self.logic_plan,
            optimizations: self.optimizations.with_compile_expressions(compiled),
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
            optimizations: self.optimizations,
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
            optimizations: OptimizationOptions::default(),
        }
    }
}
