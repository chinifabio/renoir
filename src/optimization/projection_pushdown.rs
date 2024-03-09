use crate::optimization::dsl::expressions::Expr;

use super::{
    logical_plan::LogicPlan,
    optimizer::{OptimizationRule, OptimizerError},
};

pub(crate) struct ProjectionPushdown {}

impl ProjectionPushdown {
    fn pushdown(
        plan: LogicPlan,
        accumulator: &mut Vec<(usize, usize)>,
    ) -> Result<LogicPlan, OptimizerError> {
        match plan {
            LogicPlan::TableScan {
                path,
                predicate,
                projections,
                schema,
            } => {
                if let Some(projections) = projections {
                    Self::accumulate_dependencies(accumulator, projections);
                }
                let new_projections = accumulator.iter().map(|(_, x)| *x).collect();

                // compute the mapping: the columns x become the indices i of x in the
                // accumulator
                accumulator
                    .iter_mut()
                    .enumerate()
                    .for_each(|(i, (_, x))| *x = i);

                Ok(LogicPlan::TableScan {
                    path: path.to_path_buf(),
                    predicate,
                    projections: Some(new_projections),
                    schema,
                })
            }
            LogicPlan::Select { columns, input } => {
                let mut new_accumulator = Vec::new();
                for item in &columns {
                    Self::accumulate_dependencies(
                        &mut new_accumulator,
                        item.extract_dependencies(),
                    );
                }

                let new_input = Self::pushdown(*input, &mut new_accumulator)?;
                let new_columns = columns
                    .into_iter()
                    .map(|item| Self::replace_dependencies(item, &new_accumulator))
                    .collect();

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

                let new_input = Self::pushdown(*input, accumulator)?;
                let new_predicate = Self::replace_dependencies(predicate, accumulator);

                Ok(LogicPlan::Filter {
                    predicate: new_predicate,
                    input: Box::new(new_input),
                })
            }
            LogicPlan::Shuffle { input } => {
                let new_input = Self::pushdown(*input, accumulator)?;
                Ok(LogicPlan::Shuffle {
                    input: Box::new(new_input),
                })
            }
            LogicPlan::GroupBy { key, input } => {
                for item in &key {
                    Self::accumulate_dependencies(accumulator, item.extract_dependencies());
                }
                let new_input = Self::pushdown(*input, accumulator)?;
                let new_key = key
                    .into_iter()
                    .map(|item| Self::replace_dependencies(item, accumulator))
                    .collect();
                Ok(LogicPlan::GroupBy {
                    key: new_key,
                    input: Box::new(new_input),
                })
            }
            LogicPlan::DropKey { input } => {
                let new_input = Self::pushdown(*input, accumulator)?;
                Ok(LogicPlan::DropKey {
                    input: Box::new(new_input),
                })
            }
            LogicPlan::Mean { input, skip_na } => {
                let new_input = Self::pushdown(*input, accumulator)?;
                Ok(LogicPlan::Mean {
                    input: Box::new(new_input),
                    skip_na,
                })
            }
            LogicPlan::CollectVec { input } => {
                let new_input = Self::pushdown(*input, accumulator)?;
                Ok(LogicPlan::CollectVec {
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
                // Get the length of the header of the left input
                let left_header_len = input_left.get_schema().columns.len();

                // Initialize accumulators for the left and right inputs
                let mut left_accumulator = Vec::new();
                let mut right_accumulator = Vec::new();

                // Distribute the indices in the accumulator to the left and right accumulators
                for (i, _) in accumulator.drain(..) {
                    if i < left_header_len {
                        left_accumulator.push((i, i));
                    } else {
                        let j = i - left_header_len;
                        right_accumulator.push((j, j));
                    }
                }

                // Extract dependencies from the left and right inputs and accumulate them
                for item in &left_on {
                    Self::accumulate_dependencies(
                        &mut left_accumulator,
                        item.extract_dependencies(),
                    );
                }

                for item in &right_on {
                    Self::accumulate_dependencies(
                        &mut right_accumulator,
                        item.extract_dependencies(),
                    );
                }

                // Push down the accumulators to the left and right inputs
                let new_input_left = Self::pushdown(*input_left, &mut left_accumulator)?;
                let new_input_right = Self::pushdown(*input_right, &mut right_accumulator)?;

                // Replace dependencies in the left and right inputs
                let new_left_on = left_on
                    .into_iter()
                    .map(|item| Self::replace_dependencies(item, &left_accumulator))
                    .collect();
                let new_right_on = right_on
                    .into_iter()
                    .map(|item| Self::replace_dependencies(item, &right_accumulator))
                    .collect();

                // Merge the left and right accumulators back into the main accumulator
                accumulator.extend(left_accumulator);
                let accumulator_len = accumulator.len();
                accumulator.extend(
                    right_accumulator
                        .into_iter()
                        .map(|(a, b)| (a + left_header_len, b + accumulator_len)),
                );

                // Return a new Join logic plan
                Ok(LogicPlan::Join {
                    input_left: Box::new(new_input_left),
                    input_right: Box::new(new_input_right),
                    left_on: new_left_on,
                    right_on: new_right_on,
                    join_type,
                })
            }
            LogicPlan::UpStream { stream, schema } => Ok(LogicPlan::UpStream { stream, schema }),
        }
    }

    fn accumulate_dependencies(accumulator: &mut Vec<(usize, usize)>, items: Vec<usize>) {
        accumulator.extend(items.into_iter().map(|x| (x, x)));
        accumulator.sort();
        accumulator.dedup();
    }

    /// Replace the dependencies in an expression with the new indices
    /// provided by the accumulator
    ///
    /// Filter(col(5).eq(col(4))) with accumulator [1, 4, 5] becomes Filter(col(2).eq(col(1)))
    /// because the accumulator contains the mapping [
    ///     (2, 1),
    ///     (3, 2),
    /// ]
    fn replace_dependencies(item: Expr, accumulator: &[(usize, usize)]) -> Expr {
        match item {
            Expr::NthColumn(index) => {
                let new_index = accumulator
                    .iter()
                    .find(|(old_index, _)| *old_index == index)
                    .unwrap()
                    .1;
                Expr::NthColumn(new_index)
            }
            Expr::Literal(_) => item,
            Expr::BinaryExpr { left, op, right } => Expr::BinaryExpr {
                left: Box::new(Self::replace_dependencies(*left, accumulator)),
                op,
                right: Box::new(Self::replace_dependencies(*right, accumulator)),
            },
            Expr::UnaryExpr { op, expr } => Expr::UnaryExpr {
                op,
                expr: Box::new(Self::replace_dependencies(*expr, accumulator)),
            },
            Expr::AggregateExpr { op, expr } => Expr::AggregateExpr {
                op,
                expr: Box::new(Self::replace_dependencies(*expr, accumulator)),
            },
            Expr::Empty => panic!("Can't replace dependencies in an empty expression"),
            Expr::Compiled { .. } => panic!("Can't replace dependencies in a compiled expression"),
        }
    }
}

impl OptimizationRule for ProjectionPushdown {
    fn optimize(plan: LogicPlan) -> Result<LogicPlan, OptimizerError> {
        Self::pushdown(plan, &mut vec![])
    }
}

#[cfg(test)]
pub mod test {

    use crate::data_type::noir_type::NoirTypeKind;
    use crate::data_type::schema::Schema;
    use crate::optimization::dsl::expressions::*;
    use crate::optimization::logical_plan::{JoinType, LogicPlan};
    use crate::optimization::optimizer::OptimizationRule;
    use crate::optimization::projection_pushdown::ProjectionPushdown;

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
        let target = create_scan().select(&[col(2), col(3)]).collect_vec();

        let optimized = ProjectionPushdown::optimize(target).unwrap();

        let expected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::Select {
                columns: vec![col(0), col(1)],
                input: Box::new(LogicPlan::TableScan {
                    path: "test.csv".into(),
                    predicate: None,
                    projections: Some(vec![2, 3]),
                    schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                }),
            }),
        };

        assert_eq!(expected, optimized);
    }

    #[test]
    fn pushdown_with_dependecies() {
        let target = create_scan()
            .filter(col(3).eq(i(0)))
            .select(&[col(0), col(1)])
            .collect_vec();

        let optimized = ProjectionPushdown::optimize(target).unwrap();

        let expected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::Select {
                columns: vec![col(0), col(1)],
                input: Box::new(LogicPlan::Filter {
                    predicate: col(2).eq(i(0)),
                    input: Box::new(LogicPlan::TableScan {
                        path: "test.csv".into(),
                        predicate: None,
                        projections: Some(vec![0, 1, 3]),
                        schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                    }),
                }),
            }),
        };

        assert_eq!(expected, optimized);
    }

    #[test]
    fn multiple_accumulator() {
        let target = create_scan() // -> [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            .filter(col(4).eq(i(0)))
            .select(&[col(1), col(2), col(3)]) // -> [0, 1, 2, 3]
            .filter(col(3).eq(i(0)))
            .select(&[col(1), col(2)]) // -> [0, 1]
            .collect_vec();

        let optimized = ProjectionPushdown::optimize(target).unwrap();

        let expected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::Select {
                columns: vec![col(1), col(2)],
                input: Box::new(LogicPlan::Filter {
                    predicate: col(3).eq(i(0)),
                    input: Box::new(LogicPlan::Select {
                        columns: vec![col(0), col(1), col(2)],
                        input: Box::new(LogicPlan::Filter {
                            predicate: col(3).eq(i(0)),
                            input: Box::new(LogicPlan::TableScan {
                                path: "test.csv".into(),
                                predicate: None,
                                projections: Some(vec![1, 2, 3, 4]),
                                schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                            }),
                        }),
                    }),
                }),
            }),
        };

        assert_eq!(expected, optimized);
    }

    #[test]
    fn double_select() {
        let target = create_scan()
            .select(&[col(3), col(4), col(5)])
            .select(&[col(1), col(2)])
            .collect_vec();

        let optimized = ProjectionPushdown::optimize(target).unwrap();

        let expected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::Select {
                columns: vec![col(1), col(2)],
                input: Box::new(LogicPlan::Select {
                    columns: vec![col(0), col(1), col(2)],
                    input: Box::new(LogicPlan::TableScan {
                        path: "test.csv".into(),
                        predicate: None,
                        projections: Some(vec![3, 4, 5]),
                        schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                    }),
                }),
            }),
        };

        assert_eq!(expected, optimized);
    }

    #[test]
    fn pushdow_join() {
        let a = create_scan();
        let b = create_scan().select([col(8), col(9)]);
        let target = a
            .join(
                b,
                [col(0).modulo(i(7))],
                [col(0).modulo(i(11))],
                JoinType::Inner,
            )
            .filter(col(0).eq(col(5)))
            .collect_vec();

        let optimized = ProjectionPushdown::optimize(target).unwrap();

        let expected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::Filter {
                predicate: col(0).eq(col(1)),
                input: Box::new(LogicPlan::Join {
                    input_left: Box::new(LogicPlan::TableScan {
                        path: "test.csv".into(),
                        predicate: None,
                        projections: Some(vec![0, 5]),
                        schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                    }),
                    input_right: Box::new(LogicPlan::Select {
                        columns: vec![col(0), col(1)],
                        input: Box::new(LogicPlan::TableScan {
                            path: "test.csv".into(),
                            predicate: None,
                            projections: Some(vec![8, 9]),
                            schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                        }),
                    }),
                    left_on: vec![col(0).modulo(i(7))],
                    right_on: vec![col(0).modulo(i(11))],
                    join_type: JoinType::Inner,
                }),
            }),
        };

        assert_eq!(expected, optimized);
    }

    #[test]
    fn another_join() {
        let a = create_scan();
        let b = create_scan();
        let target = a
            .join(b, [col(0), col(1)], [col(0), col(1)], JoinType::Inner)
            .filter(col(2).eq(col(12)))
            .collect_vec();

        let optimized = ProjectionPushdown::optimize(target).unwrap();

        let expected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::Filter {
                predicate: col(2).eq(col(5)),
                input: Box::new(LogicPlan::Join {
                    input_left: Box::new(LogicPlan::TableScan {
                        path: "test.csv".into(),
                        predicate: None,
                        projections: Some(vec![0, 1, 2]),
                        schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                    }),
                    input_right: Box::new(LogicPlan::TableScan {
                        path: "test.csv".into(),
                        predicate: None,
                        projections: Some(vec![0, 1, 2]),
                        schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                    }),
                    left_on: vec![col(0), col(1)],
                    right_on: vec![col(0), col(1)],
                    join_type: JoinType::Inner,
                }),
            }),
        };

        assert_eq!(expected, optimized);
    }

    #[test]
    fn pushdown_groupby() {
        let target = create_scan()
            .group_by([col(0).modulo(i(2))])
            .select([sum(col(1))])
            .collect_vec();

        let optimized = ProjectionPushdown::optimize(target).unwrap();

        let expected = LogicPlan::CollectVec {
            input: Box::new(LogicPlan::Select {
                columns: vec![sum(col(1))],
                input: Box::new(LogicPlan::GroupBy {
                    key: vec![col(0).modulo(i(2))],
                    input: Box::new(LogicPlan::TableScan {
                        path: "test.csv".into(),
                        predicate: None,
                        projections: Some(vec![0, 1]),
                        schema: Some(Schema::same_type(10, NoirTypeKind::Int32)),
                    }),
                }),
            }),
        };

        assert_eq!(expected, optimized);
    }
}
