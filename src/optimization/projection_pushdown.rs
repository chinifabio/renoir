use itertools::Itertools;

use crate::optimization::dsl::expressions::Expr;

use super::{
    logical_plan::LogicPlan,
    optimizer::{OptimizationRule, OptimizerError},
};

pub(crate) struct ProjectionPushdown<'a> {
    plan: &'a LogicPlan,
}

impl<'a> ProjectionPushdown<'a> {
    pub(crate) fn new(plan: &'a LogicPlan) -> Self {
        Self { plan }
    }

    fn pushdown(
        plan: &LogicPlan,
        accumulator: &mut Vec<usize>,
    ) -> Result<LogicPlan, OptimizerError> {
        match plan {
            LogicPlan::TableScan {
                path,
                predicate,
                projections,
            } => {
                if projections.is_some() {
                    Self::accumulate_dependencies(accumulator, projections.clone().unwrap());
                }
                Ok(LogicPlan::TableScan {
                    path: path.to_path_buf(),
                    predicate: predicate.clone(),
                    projections: Some(accumulator.clone()),
                })
            }
            LogicPlan::Select { columns, input } => {
                let mut new_accumulator = columns
                    .iter()
                    .flat_map(|e| e.extract_dependencies())
                    .unique()
                    .collect_vec();

                let new_input = Self::pushdown(input, &mut new_accumulator)?;
                let new_columns = columns
                    .iter()
                    .map(|item| Self::replace_dependencies(item, &new_accumulator))
                    .collect_vec();

                Ok(LogicPlan::Select {
                    columns: new_columns,
                    input: Box::new(new_input),
                })
            }
            LogicPlan::DropColumns {
                input: _,
                columns: _,
            } => Err(OptimizerError::NotImplemented {
                message: "Missing schema from which remove the columns".to_string(),
            }),
            LogicPlan::Filter { predicate, input } => {
                Self::accumulate_dependencies(accumulator, predicate.extract_dependencies());

                let new_input = Self::pushdown(input, accumulator)?;
                let new_predicate = Self::replace_dependencies(predicate, accumulator);

                Ok(LogicPlan::Filter {
                    predicate: new_predicate,
                    input: Box::new(new_input),
                })
            }
            LogicPlan::Shuffle { input } => {
                let new_input = Self::pushdown(input, accumulator)?;
                Ok(LogicPlan::Shuffle {
                    input: Box::new(new_input),
                })
            }
            LogicPlan::GroupBy { key, input } => {
                Self::accumulate_dependencies(accumulator, key.extract_dependencies());
                let new_input = Self::pushdown(input, accumulator)?;
                let new_key = Self::replace_dependencies(key, accumulator);
                Ok(LogicPlan::GroupBy {
                    key: new_key,
                    input: Box::new(new_input),
                })
            }
            LogicPlan::DropKey { input } => {
                let new_input = Self::pushdown(input, accumulator)?;
                Ok(LogicPlan::DropKey {
                    input: Box::new(new_input),
                })
            }
            LogicPlan::CollectVec { input } => {
                let new_input = Self::pushdown(input, accumulator)?;
                Ok(LogicPlan::CollectVec {
                    input: Box::new(new_input),
                })
            }
        }
    }

    fn accumulate_dependencies(accumulator: &mut Vec<usize>, items: Vec<usize>) {
        accumulator.extend(items);
        accumulator.sort();
        accumulator.dedup();
    }

    fn replace_dependencies(item: &Expr, new_accumulator: &[usize]) -> Expr {
        match item {
            Expr::NthColumn(index) => {
                let new_index = new_accumulator.iter().position(|&x| x == *index).unwrap();
                Expr::NthColumn(new_index)
            }
            Expr::Literal(_) => item.clone(),
            Expr::BinaryExpr { left, op, right } => Expr::BinaryExpr {
                left: Box::new(Self::replace_dependencies(left, new_accumulator)),
                op: *op,
                right: Box::new(Self::replace_dependencies(right, new_accumulator)),
            },
            Expr::UnaryExpr { op, expr } => Expr::UnaryExpr {
                op: *op,
                expr: Box::new(Self::replace_dependencies(expr, new_accumulator)),
            },
            Expr::Empty => panic!("Empty expression"),
        }
    }
}

impl<'a> OptimizationRule for ProjectionPushdown<'a> {
    fn optimize(&self) -> Result<LogicPlan, OptimizerError> {
        Self::pushdown(self.plan, &mut vec![])
    }
}

#[cfg(test)]
pub mod test {

    use crate::optimization::optimizer::OptimizationRule;
    use crate::optimization::dsl::expressions::*;
    use crate::optimization::logical_plan::LogicPlan;
    use crate::optimization::projection_pushdown::ProjectionPushdown;

    fn create_scan() -> LogicPlan {
        LogicPlan::TableScan {
            path: "test.csv".into(),
            predicate: None,
            projections: None,
        }
    }

    #[test]
    fn simple_pushdown() {
        let target = create_scan()
            .select(&[col(0), col(1)])
            .collect_vec();

        let optimized = ProjectionPushdown::new(&target).optimize().unwrap();

        let expected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::Select {
                columns: vec![col(0), col(1)],
                input: Box::new(LogicPlan::TableScan {
                    path: "test.csv".into(),
                    predicate: None,
                    projections: Some(vec![0, 1]),
                }),
            })
        };

        assert_eq!(expected, optimized);
    }

    #[test]
    fn pushdown_with_dependecies() {
        let target = create_scan()
            .filter(col(3).eq(i(0)))
            .select(&[col(0), col(1)])
            .collect_vec();

        let optimized = ProjectionPushdown::new(&target).optimize().unwrap();

        let expected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::Select {
                columns: vec![col(0), col(1)],
                input: Box::new(LogicPlan::Filter {
                    predicate: col(2).eq(i(0)),
                    input: Box::new(LogicPlan::TableScan {
                        path: "test.csv".into(),
                        predicate: None,
                        projections: Some(vec![0, 1, 3]),
                    }),
                }),
            })
        };

        assert_eq!(expected, optimized);
    }
}
