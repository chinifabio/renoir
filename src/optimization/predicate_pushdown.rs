use std::vec;

use itertools::Itertools;

use super::dsl::expressions::{Expr, ExprOp};

use super::{
    logical_plan::LogicPlan,
    optimizer::{OptimizationRule, OptimizerError, OptimizerResult},
};

pub(crate) struct PredicatePushdown<'a> {
    plan: &'a LogicPlan,
}

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
        self.predicate.is_some() && !self.is_locked() && self.level < i
    }

    fn take(&mut self, i: u32) -> Self {
        assert!(self.is_available(i), "Cannot take a locked predicate");
        Self {
            predicate: self.predicate.take(),
            level: i,
            locked_at: None,
        }
    }
}

impl<'a> PredicatePushdown<'a> {
    pub(crate) fn new(plan: &'a LogicPlan) -> Self {
        Self { plan }
    }

    fn pushdown(
        plan: &LogicPlan,
        accumulator: &mut Vec<PredicateWrapper>,
        i: u32,
    ) -> Result<LogicPlan, OptimizerError> {
        match plan {
            LogicPlan::TableScan {
                path,
                predicate,
                projections,
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
                    projections: projections.clone(),
                })
            }
            // TODO controllare due filter consecutivi
            LogicPlan::Filter { predicate, input } => {
                let previous = accumulator
                    .iter_mut()
                    .filter(|p| p.is_available(i))
                    .map(|p| p.take(i))
                    .collect_vec();
                let pieces = Self::separate(predicate, i);
                let start = accumulator.len();
                let len = pieces.len() + previous.len();
                accumulator.extend(pieces);
                accumulator.extend(previous);
                let new_input = Self::pushdown(input, accumulator, i + 1)?;
                let new_predicate = accumulator[start..len + start]
                    .iter_mut()
                    .filter(|p| p.is_available(i))
                    .map(|p| p.predicate.take().unwrap())
                    .reduce(|acc, item| acc.and(item));
                match new_predicate {
                    Some(predicate) => Ok(LogicPlan::Filter {
                        predicate,
                        input: Box::new(new_input),
                    }),
                    None => Ok(new_input),
                }
            }
            LogicPlan::Select { columns, input } => {
                // per semplificare, non controllo quali predicati possano scendere ancora.
                // TODO: guardare cosa può scendere ancora
                accumulator.iter_mut().for_each(|p| p.lock(i));
                let mut new_input = Self::pushdown(input, accumulator, i + 1)?;
                accumulator.iter_mut().for_each(|p| p.unlock(i));
                match Self::take_action(accumulator, i) {
                    Some(predicate) => {
                        new_input = LogicPlan::Select {
                            columns: columns.clone(),
                            input: Box::new(new_input),
                        };
                        Ok(LogicPlan::Filter {
                            predicate,
                            input: Box::new(new_input),
                        })
                    }
                    None => Ok(LogicPlan::Select {
                        columns: columns.clone(),
                        input: Box::new(new_input),
                    }),
                }
            }
            LogicPlan::DropColumns { input, columns } => {
                // TODO come select
                let new_input = Self::pushdown(input, accumulator, i + 1)?;
                Ok(LogicPlan::DropColumns {
                    input: Box::new(new_input),
                    columns: columns.clone(),
                })
            }
            LogicPlan::CollectVec { input } => {
                let new_input = Self::pushdown(input, accumulator, i + 1)?;
                Ok(LogicPlan::CollectVec {
                    input: Box::new(new_input),
                })
            }
            LogicPlan::Shuffle { input } => {
                let new_input = Self::pushdown(input, accumulator, i + 1)?;
                Ok(LogicPlan::Shuffle {
                    input: Box::new(new_input),
                })
            }
            LogicPlan::GroupBy { key, input } => {
                let new_input = Self::pushdown(input, accumulator, i + 1)?;
                Ok(LogicPlan::GroupBy {
                    key: key.clone(),
                    input: Box::new(new_input),
                })
            }
            LogicPlan::DropKey { input } => {
                let new_input = Self::pushdown(input, accumulator, i + 1)?;
                Ok(LogicPlan::DropKey {
                    input: Box::new(new_input),
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

    fn separate(input: &Expr, i: u32) -> Vec<PredicateWrapper> {
        let mut stack = Vec::new();
        let mut iter = Some(input);

        while let Some(predicate) = iter {
            match predicate {
                Expr::BinaryExpr {
                    left,
                    right,
                    op: ExprOp::And,
                } => {
                    iter = Some(left);
                    stack.push(&(**right));
                }
                _ => {
                    iter = None;
                    stack.push(predicate);
                }
            }
        }

        stack
            .into_iter()
            .map(|p| PredicateWrapper::new(p.clone(), i))
            .collect()
    }
}

impl<'a> OptimizationRule for PredicatePushdown<'a> {
    fn optimize(&self) -> OptimizerResult {
        Self::pushdown(self.plan, &mut vec![], 0)
    }
}

#[cfg(test)]
pub mod test {

    use crate::optimization::dsl::expressions::*;
    use crate::optimization::logical_plan::LogicPlan;
    use crate::optimization::optimizer::OptimizationRule;
    use crate::optimization::predicate_pushdown::{PredicatePushdown, PredicateWrapper};

    #[test]
    fn test_separate() {
        let expr = (col(0) + col(1)).eq(i(0)) & col(0).neq(f(0.0)) & col(1);
        let pieces = PredicatePushdown::separate(&expr, 0);
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
        let mut pieces = PredicatePushdown::separate(&expr, 0);
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
        }
    }

    #[test]
    fn simple_pushdown() {
        let target_plan = create_scan().filter(col(0).gt(i(0))).collect_vec();

        let optimized = PredicatePushdown::new(&target_plan).optimize().unwrap();

        let exprected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::TableScan {
                path: "test.csv".into(),
                predicate: Some(col(0).gt(i(0))),
                projections: None,
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

        let optimized = PredicatePushdown::new(&target_plan).optimize().unwrap();

        let exprected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::TableScan {
                path: "test.csv".into(),
                predicate: Some(col(0).gt(i(0)) & col(1).lt(i(0))),
                projections: None,
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

        let optimized = PredicatePushdown::new(&target_plan).optimize().unwrap();

        let exprected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::TableScan {
                path: "test.csv".into(),
                predicate: Some(col(0).gt(i(0)) & col(1).lt(i(0)) & col(2).eq(i(0))),
                projections: None,
            }),
        };

        assert_eq!(optimized, exprected);
    }

    #[test]
    fn keyed_stream() {
        let target_plan = create_scan()
            .filter(col(0).gt(i(0)))
            .group_by(col(1))
            .filter(col(1).lt(i(0)))
            .collect_vec();

        let optimized = PredicatePushdown::new(&target_plan).optimize().unwrap();

        let exprected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::GroupBy {
                key: col(1),
                input: Box::new(LogicPlan::TableScan {
                    path: "test.csv".into(),
                    predicate: Some(col(0).gt(i(0)) & col(1).lt(i(0))),
                    projections: None,
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

        let optimized = PredicatePushdown::new(&target_plan).optimize().unwrap();

        let exprected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::Filter {
                predicate: col(1).lt(i(0)),
                input: Box::new(LogicPlan::Select {
                    columns: vec![col(0), col(1)],
                    input: Box::new(LogicPlan::TableScan {
                        path: "test.csv".into(),
                        predicate: Some(col(0).gt(i(0))),
                        projections: None,
                    }),
                }),
            }),
        };

        assert_eq!(optimized, exprected);
    }
}
