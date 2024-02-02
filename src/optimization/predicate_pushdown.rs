use std::vec;

use itertools::Itertools;

use super::dsl::expressions::{Expr, ExprOp};

use super::{
    logical_plan::LogicPlan,
    optimizer::{OptimizationRule, OptimizerError, OptimizerResult},
};

pub(crate) struct PredicatePushdown {}

#[derive(Debug, PartialEq)]
struct PredicateWrapper {
    predicate: Option<Expr>,
    level: u32,
    locked_at: Option<u32>,
}

impl PredicateWrapper {
    fn new(predicate: Expr, i: u32) -> Self {
        Self {
            predicate: Some(predicate),
            level: i,
            locked_at: None,
        }
    }

    fn lock(&mut self, i: u32) {
        self.locked_at = Some(i);
    }

    fn unlock(&mut self, i: u32) {
        if self.locked_at == Some(i) {
            self.locked_at = None;
        }
    }

    fn is_locked(&self) -> bool {
        self.locked_at.is_some()
    }

    fn is_available(&self, i: u32) -> bool {
        self.predicate.is_some() && !self.is_locked() && self.level <= i
    }

    fn take(&mut self, i: u32) -> Self {
        assert!(self.is_available(i), "Cannot take a locked predicate");
        Self {
            predicate: self.predicate.take(),
            level: i,
            locked_at: None,
        }
    }

    fn is_left(&self, level: u32, left_len: usize) -> bool {
        if self.is_available(level) {
            return self
                .predicate
                .as_ref()
                .unwrap()
                .extract_dependencies()
                .iter()
                .all(|d| *d < left_len);
        }
        false
    }

    fn is_right(&self, level: u32, left_len: usize) -> bool {
        if self.is_available(level) {
            return self
                .predicate
                .as_ref()
                .unwrap()
                .extract_dependencies()
                .iter()
                .all(|d| *d >= left_len);
        }
        false
    }

    fn shift_left(self, amount: usize) -> PredicateWrapper {
        let predicate = self.predicate.unwrap().shift_left(amount);
        PredicateWrapper {
            predicate: Some(predicate),
            level: self.level,
            locked_at: self.locked_at,
        }
    }
}

impl PredicatePushdown {
    fn pushdown(
        plan: LogicPlan,
        accumulator: &mut Vec<PredicateWrapper>,
        i: u32,
    ) -> Result<LogicPlan, OptimizerError> {
        match plan {
            LogicPlan::TableScan {
                path,
                predicate,
                projections,
                schema,
            } => {
                let new_predicate = match (predicate.to_owned(), Self::take_action(accumulator, i))
                {
                    (Some(p), Some(q)) => Some(p.and(q)),
                    (Some(p), None) => Some(p),
                    (None, Some(q)) => Some(q),
                    (None, None) => None,
                };
                Ok(LogicPlan::TableScan {
                    path: path.to_path_buf(),
                    predicate: new_predicate,
                    projections,
                    schema,
                })
            }
            LogicPlan::Filter { predicate, input } => {
                // Takes ownership of the upstream predicates
                let upstream_predicates = accumulator
                    .iter_mut()
                    .filter(|p| p.is_available(i))
                    .map(|p| p.take(i))
                    .collect_vec();

                // Separates the predicate in the current level into its components
                // a < b & c > d => [a < b, c > d]
                let predicate_components = Self::separate(predicate, i);

                // Pushes the upstream predicates and the predicate components into the accumulator
                let start = accumulator.len();
                let len = predicate_components.len() + upstream_predicates.len();
                accumulator.extend(predicate_components);
                accumulator.extend(upstream_predicates);

                // Recursively pushdown the input
                let new_input = Self::pushdown(*input, accumulator, i + 1)?;

                // Recover the pushed predicates: if the predicate is locked, it means that it has been
                // pushed down to a lower level, so it is not available anymore
                let new_predicate = Self::take_action(&mut accumulator[start..(start + len)], i);
                match new_predicate {
                    Some(predicate) => Ok(LogicPlan::Filter {
                        predicate,
                        input: Box::new(new_input),
                    }),
                    None => Ok(new_input),
                }
            }
            LogicPlan::Select { columns, input } => {
                // TODO devo controllare se tutte le dipendenze del filter sono colonne semplici.
                // se lo sono allora posso rimappare le dipendenze e porare su il filtro
                accumulator.iter_mut().for_each(|p| p.lock(i));
                let mut new_input = Self::pushdown(*input, accumulator, i + 1)?;
                accumulator.iter_mut().for_each(|p| p.unlock(i));
                match Self::take_action(accumulator, i) {
                    Some(predicate) => {
                        new_input = LogicPlan::Select {
                            columns,
                            input: Box::new(new_input),
                        };
                        Ok(LogicPlan::Filter {
                            predicate,
                            input: Box::new(new_input),
                        })
                    }
                    None => Ok(LogicPlan::Select {
                        columns,
                        input: Box::new(new_input),
                    }),
                }
            }
            LogicPlan::DropColumns { input, columns } => {
                // TODO come select
                let new_input = Self::pushdown(*input, accumulator, i + 1)?;
                Ok(LogicPlan::DropColumns {
                    input: Box::new(new_input),
                    columns,
                })
            }
            LogicPlan::CollectVec { input } => {
                let new_input = Self::pushdown(*input, accumulator, i + 1)?;
                Ok(LogicPlan::CollectVec {
                    input: Box::new(new_input),
                })
            }
            LogicPlan::Shuffle { input } => {
                let new_input = Self::pushdown(*input, accumulator, i + 1)?;
                Ok(LogicPlan::Shuffle {
                    input: Box::new(new_input),
                })
            }
            LogicPlan::GroupBy { key, input } => {
                let new_input = Self::pushdown(*input, accumulator, i + 1)?;
                Ok(LogicPlan::GroupBy {
                    key,
                    input: Box::new(new_input),
                })
            }
            LogicPlan::DropKey { input } => {
                let new_input = Self::pushdown(*input, accumulator, i + 1)?;
                Ok(LogicPlan::DropKey {
                    input: Box::new(new_input),
                })
            }
            LogicPlan::Join {
                input_left,
                input_right,
                left_on,
                right_on,
                join_type,
            } => {
                // Divide the predicates in the accumulator into two groups: those that depend only on the left
                // input and those that depend only on the right input
                let left_header_len = input_left.schema().columns.len();
                let mut left_accumulator = accumulator
                    .iter_mut()
                    .filter(|p| p.is_left(i, left_header_len))
                    .map(|p| p.take(i))
                    .collect_vec();
                let mut right_accumulator = accumulator
                    .iter_mut()
                    .filter(|p| p.is_right(i, left_header_len))
                    .map(|p| p.take(i))
                    .map(|p| p.shift_left(left_header_len))
                    .collect_vec();

                // Pushdown the left and right inputs
                let new_input_left = Self::pushdown(*input_left, &mut left_accumulator, i + 1)?;
                let new_input_right = Self::pushdown(*input_right, &mut right_accumulator, i + 1)?;

                // No need to recover the pushed predicates, since they surely are taken  by upstream operators
                Ok(LogicPlan::Join {
                    input_left: Box::new(new_input_left),
                    input_right: Box::new(new_input_right),
                    left_on,
                    right_on,
                    join_type,
                })
            }
        }
    }

    fn take_action(accumulator: &mut [PredicateWrapper], i: u32) -> Option<Expr> {
        accumulator
            .iter_mut()
            .filter(|p| p.is_available(i))
            .map(|p| p.predicate.take().unwrap())
            .reduce(|acc, item| acc.and(item))
    }

    fn separate(input: Expr, i: u32) -> Vec<PredicateWrapper> {
        let mut stack = Vec::new();
        let mut iter = Some(input);

        while let Some(predicate) = iter {
            match predicate {
                Expr::BinaryExpr {
                    left,
                    right,
                    op: ExprOp::And,
                } => {
                    iter = Some(*left);
                    stack.push(*right);
                }
                _ => {
                    iter = None;
                    stack.push(predicate);
                }
            }
        }

        stack
            .into_iter()
            .map(|p| PredicateWrapper::new(p, i))
            .collect()
    }
}

impl OptimizationRule for PredicatePushdown {
    fn optimize(plan: LogicPlan) -> OptimizerResult {
        Self::pushdown(plan, &mut vec![], 0)
    }
}

#[cfg(test)]
pub mod test {

    use crate::data_type::{NoirTypeKind, Schema};
    use crate::optimization::dsl::expressions::*;
    use crate::optimization::logical_plan::{JoinType, LogicPlan};
    use crate::optimization::optimizer::OptimizationRule;
    use crate::optimization::predicate_pushdown::{PredicatePushdown, PredicateWrapper};

    #[test]
    fn test_separate() {
        let expr = (col(0) + col(1)).eq(i(0)) & col(0).neq(f(0.0)) & col(1);
        let pieces = PredicatePushdown::separate(expr, 0);
        let expected = vec![
            PredicateWrapper::new(col(1), 0),
            PredicateWrapper::new(col(0).neq(f(0.0)), 0),
            PredicateWrapper::new((col(0) + col(1)).eq(i(0)), 0),
        ];
        assert_eq!(pieces, expected);
    }

    #[test]
    fn test_separate_2() {
        let expr = (col(0) + col(1)).eq(i(0)) & col(0).neq(f(0.0)) | col(1) & col(2).eq(i(0));
        let mut pieces = PredicatePushdown::separate(expr.clone(), 0);
        assert_eq!(expr, pieces[0].predicate.take().unwrap());
    }

    #[test]
    fn test_take_action() {
        let mut accumulator = vec![
            PredicateWrapper {
                predicate: None,
                level: 1,
                locked_at: None,
            },
            PredicateWrapper {
                predicate: Some((col(0) + 1).eq(i(3))),
                level: 3,
                locked_at: None,
            },
            PredicateWrapper {
                predicate: Some((col(1) / col(2)).neq(f(1.5))),
                level: 3,
                locked_at: None,
            },
        ];
        let predicate = PredicatePushdown::take_action(&mut accumulator, 4);
        let expected = Some((col(0) + 1).eq(i(3)) & (col(1) / col(2)).neq(f(1.5)));
        assert_eq!(predicate, expected);
    }

    fn create_scan() -> LogicPlan {
        LogicPlan::TableScan {
            path: "test.csv".into(),
            predicate: None,
            projections: None,
            schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
        }
    }

    #[test]
    fn simple_pushdown() {
        let target_plan = create_scan().filter(col(0).gt(i(0))).collect_vec();

        let optimized = PredicatePushdown::optimize(target_plan).unwrap();

        let exprected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::TableScan {
                path: "test.csv".into(),
                predicate: Some(col(0).gt(i(0))),
                projections: None,
                schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
            }),
        };

        assert_eq!(optimized, exprected);
    }

    #[test]
    fn consecutive_filters() {
        let target_plan = create_scan()
            .filter(col(0).gt(i(0)))
            .filter(col(1).lt(i(0)))
            .collect_vec();

        let optimized = PredicatePushdown::optimize(target_plan).unwrap();

        let exprected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::TableScan {
                path: "test.csv".into(),
                predicate: Some(col(0).gt(i(0)) & col(1).lt(i(0))),
                projections: None,
                schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
            }),
        };

        assert_eq!(optimized, exprected);
    }

    #[test]
    fn consecutive_filters_2() {
        let target_plan = create_scan()
            .filter(col(0).gt(i(0)))
            .filter(col(1).lt(i(0)))
            .filter(col(2).eq(i(0)))
            .collect_vec();

        let optimized = PredicatePushdown::optimize(target_plan).unwrap();

        let exprected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::TableScan {
                path: "test.csv".into(),
                predicate: Some(col(0).gt(i(0)) & col(1).lt(i(0)) & col(2).eq(i(0))),
                projections: None,
                schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
            }),
        };

        assert_eq!(optimized, exprected);
    }

    #[test]
    fn keyed_stream() {
        let target_plan = create_scan()
            .filter(col(0).gt(i(0)))
            .group_by([col(1)])
            .filter(col(1).lt(i(0)))
            .collect_vec();

        let optimized = PredicatePushdown::optimize(target_plan).unwrap();

        let exprected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::GroupBy {
                key: vec![col(1)],
                input: Box::new(LogicPlan::TableScan {
                    path: "test.csv".into(),
                    predicate: Some(col(0).gt(i(0)) & col(1).lt(i(0))),
                    projections: None,
                    schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                }),
            }),
        };

        assert_eq!(optimized, exprected);
    }

    #[test]
    fn stop_on_selection() {
        let target_plan = create_scan()
            .filter(col(0).gt(i(0)))
            .select(&[col(0), col(1)])
            .filter(col(1).lt(i(0)))
            .collect_vec();

        let optimized = PredicatePushdown::optimize(target_plan).unwrap();

        let exprected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::Filter {
                predicate: col(1).lt(i(0)),
                input: Box::new(LogicPlan::Select {
                    columns: vec![col(0), col(1)],
                    input: Box::new(LogicPlan::TableScan {
                        path: "test.csv".into(),
                        predicate: Some(col(0).gt(i(0))),
                        projections: None,
                        schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                    }),
                }),
            }),
        };

        assert_eq!(optimized, exprected);
    }

    #[test]
    fn filter_remain_after_join() {
        let target = create_scan()
            .join(
                create_scan(),
                &[col(0), col(1)],
                &[col(0), col(1)],
                JoinType::Inner,
            )
            .filter(col(2).eq(col(12)))
            .collect_vec();

        let optimized = PredicatePushdown::optimize(target).unwrap();

        let exprected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::Filter {
                predicate: col(2).eq(col(12)),
                input: Box::new(LogicPlan::Join {
                    input_left: Box::new(LogicPlan::TableScan {
                        path: "test.csv".into(),
                        predicate: None,
                        projections: None,
                        schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                    }),
                    input_right: Box::new(LogicPlan::TableScan {
                        path: "test.csv".into(),
                        predicate: None,
                        projections: None,
                        schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                    }),
                    left_on: vec![col(0), col(1)],
                    right_on: vec![col(0), col(1)],
                    join_type: JoinType::Inner,
                }),
            }),
        };

        assert_eq!(optimized, exprected);
    }

    #[test]
    fn split_filter_after_join() {
        let target = create_scan()
            .join(
                create_scan(),
                &[col(0), col(1)],
                &[col(0), col(1)],
                JoinType::Inner,
            )
            .filter(col(2).eq(i(10)).and(col(12).eq(i(10))))
            .collect_vec();

        let optimized = PredicatePushdown::optimize(target).unwrap();

        let exprected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::Join {
                input_left: Box::new(LogicPlan::TableScan {
                    path: "test.csv".into(),
                    predicate: Some(col(2).eq(i(10))),
                    projections: None,
                    schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                }),
                input_right: Box::new(LogicPlan::TableScan {
                    path: "test.csv".into(),
                    predicate: Some(col(2).eq(i(10))),
                    projections: None,
                    schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                }),
                left_on: vec![col(0), col(1)],
                right_on: vec![col(0), col(1)],
                join_type: JoinType::Inner,
            }),
        };

        assert_eq!(optimized, exprected);
    }
}
