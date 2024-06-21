use std::path::PathBuf;

use crate::{
    datatypes::{schema::Schema, stream_item::StreamItem, NoirType},
    dsl::expressions::Expr,
    operator::{
        boxed::BoxedOperator, filter_expr::FilterExpr, sink::StreamOutput, source::CsvOptions,
        Operator,
    },
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

    pub fn select(self, columns: Vec<Expr>) -> Stream<BoxedOperator<StreamItem>> {
        if columns.iter().any(|e| e.is_aggregator()) {
            let projections = columns.clone();
            let accumulator: Vec<_> = columns.into_iter().map(|e| e.accumulator()).collect();
            self.fold_assoc(
                accumulator,
                move |acc, value| {
                    let temp: Vec<NoirType> = projections
                        .iter()
                        .map(|expr| expr.evaluate(&value))
                        .collect();
                    for i in 0..acc.len() {
                        acc[i].accumulate(temp[i].clone());
                    }
                },
                |acc, val| {
                    for i in 0..acc.len() {
                        acc[i].include(val[i].clone())
                    }
                },
            )
            .map(|acc| {
                acc.into_iter()
                    .map(|a| a.finalize())
                    .collect::<Vec<NoirType>>()
            })
            .map(StreamItem::from)
            .into_boxed()
        } else {
            let temp_stream = self.map(move |item| {
                columns
                    .iter()
                    .map(|expr| expr.evaluate(&item))
                    .collect::<Vec<_>>()
            });
            temp_stream.map(StreamItem::from).into_boxed()
        }
    }

    pub fn group_by_expr(self, keys: Vec<Expr>) -> Stream<BoxedOperator<StreamItem>> {
        self.group_by(move |item: &StreamItem| keys.iter().map(|k| k.evaluate(item)).collect())
            .0
            .map(|(k, v)| v.absorb_key(k))
            .into_boxed()
    }
}

impl<'a> OptStream<'a> {
    pub fn collect_vec(self) -> StreamOutput<Vec<StreamItem>> {
        let optimized = self.logic_plan.collect_vec().optimize(self.optimizations);
        info!("Optimized plan: {}", optimized);
        to_stream(
            optimized,
            &self.context,
            self.csv_options.unwrap_or_default(),
        )
        .into_output()
    }

    pub fn filter(self, predicate: Expr) -> Self {
        OptStream {
            logic_plan: self.logic_plan.filter(predicate),
            ..self
        }
    }

    pub fn shuffle(self) -> Self {
        OptStream {
            logic_plan: self.logic_plan.shuffle(),
            ..self
        }
    }

    pub fn group_by<E: AsRef<[Expr]>>(self, key: E) -> Self {
        OptStream {
            logic_plan: self.logic_plan.group_by(key),
            ..self
        }
    }

    pub fn select<E: AsRef<[Expr]>>(self, exprs: E) -> Self {
        OptStream {
            logic_plan: self.logic_plan.select(exprs),
            ..self
        }
    }

    pub fn drop_key(self) -> Self {
        OptStream {
            logic_plan: self.logic_plan.drop_key(),
            ..self
        }
    }

    pub fn drop(self, cols: Vec<usize>) -> Self {
        OptStream {
            logic_plan: self.logic_plan.drop(cols),
            ..self
        }
    }

    pub fn mean(self, skip_na: bool) -> Self {
        OptStream {
            logic_plan: self.logic_plan.mean(skip_na),
            ..self
        }
    }

    pub fn join<E: AsRef<[Expr]>>(
        self,
        other: OptStream<'a>,
        left_on: E,
        right_on: E,
    ) -> OptStream {
        OptStream {
            logic_plan: self
                .logic_plan
                .join(other.logic_plan, left_on, right_on, JoinType::Inner),
            ..self
        }
    }

    pub fn left_join<E: AsRef<[Expr]>>(
        self,
        other: OptStream<'a>,
        left_on: E,
        right_on: E,
    ) -> OptStream {
        OptStream {
            logic_plan: self
                .logic_plan
                .join(other.logic_plan, left_on, right_on, JoinType::Left),
            ..self
        }
    }

    pub fn full_join<E: AsRef<[Expr]>>(
        self,
        other: OptStream<'a>,
        left_on: E,
        right_on: E,
    ) -> OptStream {
        OptStream {
            logic_plan: self
                .logic_plan
                .join(other.logic_plan, left_on, right_on, JoinType::Outer),
            ..self
        }
    }

    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.logic_plan.set_schema(schema);
        self
    }

    pub fn infer_schema(mut self, has_header: bool) -> Self {
        self.logic_plan.infer_schema(has_header);
        self
    }

    pub fn with_optimizations(self, optimizations: OptimizationOptions) -> Self {
        Self {
            optimizations,
            ..self
        }
    }

    pub fn with_compiled_expressions(self, compiled: bool) -> Self {
        Self {
            optimizations: self.optimizations.with_compile_expressions(compiled),
            ..self
        }
    }

    pub fn with_predicate_pushdown(self, predicate_pushdown: bool) -> Self {
        Self {
            optimizations: self
                .optimizations
                .with_predicate_pushdown(predicate_pushdown),
            ..self
        }
    }

    pub fn with_projection_pushdown(self, projection_pushdown: bool) -> Self {
        Self {
            optimizations: self
                .optimizations
                .with_projection_pushdown(projection_pushdown),
            ..self
        }
    }

    pub fn with_stream_rewrite(self, stream_rewrite: bool) -> Self {
        Self {
            optimizations: self.optimizations.with_stream_rewrite(stream_rewrite),
            ..self
        }
    }

    pub fn without_optimizations(self) -> Self {
        Self {
            optimizations: OptimizationOptions::none(),
            ..self
        }
    }

    pub fn with_csv_options(self, csv_options: CsvOptions) -> Self {
        Self {
            csv_options: Some(csv_options),
            ..self
        }
    }
}

impl crate::StreamContext {
    pub fn stream_csv_optimized(&mut self, path: impl Into<PathBuf>) -> OptStream {
        OptStream {
            context: self,
            logic_plan: LogicPlan::TableScan {
                path: path.into(),
                predicate: None,
                projections: None,
                schema: None,
            },
            optimizations: OptimizationOptions::default(),
            csv_options: None,
        }
    }

    pub fn optimized_from_stream(
        &mut self,
        stream: Stream<BoxedOperator<StreamItem>>,
        schema: Schema,
    ) -> OptStream {
        OptStream {
            context: self,
            logic_plan: LogicPlan::UpStream {
                stream,
                schema: Some(schema),
            },
            optimizations: OptimizationOptions::default(),
            csv_options: None,
        }
    }
}
