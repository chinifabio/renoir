use core::panic;
use std::fmt::Debug;
use std::fmt::Display;
use std::path::PathBuf;

use crate::data_type::schema::Schema;

use super::dsl::expressions::*;
use super::optimizer::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Outer,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogicPlan {
    TableScan {
        path: PathBuf,
        predicate: Option<Expr>,
        projections: Option<Vec<usize>>,
        schema: Option<Schema>,
    },
    Filter {
        predicate: Expr,
        input: Box<LogicPlan>,
    },
    Select {
        columns: Vec<Expr>,
        input: Box<LogicPlan>,
    },
    Shuffle {
        input: Box<LogicPlan>,
    },
    GroupBy {
        key: Vec<Expr>,
        input: Box<LogicPlan>,
    },
    DropKey {
        input: Box<LogicPlan>,
    },
    CollectVec {
        input: Box<LogicPlan>,
    },
    DropColumns {
        input: Box<LogicPlan>,
        columns: Vec<usize>,
    },
    Join {
        input_left: Box<LogicPlan>,
        input_right: Box<LogicPlan>,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        join_type: JoinType,
    },
}

impl Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Inner => write!(f, "Inner"),
            JoinType::Left => write!(f, "Left"),
            JoinType::Outer => write!(f, "Full"),
        }
    }
}

impl Display for LogicPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicPlan::TableScan {
                path,
                predicate,
                projections,
                ..
            } => {
                write!(
                    f,
                    "TableScan({}, {}, {:?})",
                    path.display(),
                    predicate.clone().unwrap_or(Expr::Empty),
                    projections.clone().unwrap_or_default()
                )
            }
            LogicPlan::Filter { predicate, input } => {
                write!(f, "{} -> Filter({})", input, predicate)
            }
            LogicPlan::Select { columns, input } => {
                write!(f, "{} -> Select({:?})", input, columns)
            }
            LogicPlan::Shuffle { input } => {
                write!(f, "{} -> Shuffle", input)
            }
            LogicPlan::GroupBy { key, input } => {
                write!(f, "{} -> GroupBy({:?})", input, key)
            }
            LogicPlan::DropKey { input } => {
                write!(f, "{} -> DropKey", input)
            }
            LogicPlan::CollectVec { input } => {
                write!(f, "{} -> CollectVec", input)
            }
            LogicPlan::DropColumns { input, columns } => {
                write!(f, "{} -> DropColumns({:?})", input, columns)
            }
            LogicPlan::Join {
                input_left,
                input_right,
                left_on,
                right_on,
                join_type,
            } => {
                write!(
                    f,
                    "{}\n{}\n\t-> {}Join({:?}, {:?})",
                    input_left, input_right, join_type, left_on, right_on
                )
            }
        }
    }
}

impl LogicPlan {
    pub(crate) fn optimize(self, options: OptimizationOptions) -> LogicPlan {
        match LogicPlanOptimizer::optmize_with_options(self, options) {
            Ok(plan) => plan,
            Err(err) => panic!("Error during optimization: {}", err),
        }
    }

    pub(crate) fn filter(self, predicate: Expr) -> LogicPlan {
        LogicPlan::Filter {
            predicate,
            input: Box::new(self),
        }
    }

    pub(crate) fn shuffle(self) -> LogicPlan {
        LogicPlan::Shuffle {
            input: Box::new(self),
        }
    }

    pub(crate) fn group_by<E: AsRef<[Expr]>>(self, key: E) -> LogicPlan {
        LogicPlan::GroupBy {
            key: key.as_ref().to_vec(),
            input: Box::new(self),
        }
    }

    pub(crate) fn drop_key(self) -> LogicPlan {
        LogicPlan::DropKey {
            input: Box::new(self),
        }
    }

    pub(crate) fn select<E: AsRef<[Expr]>>(self, columns: E) -> LogicPlan {
        LogicPlan::Select {
            columns: columns.as_ref().to_vec(),
            input: Box::new(self),
        }
    }

    pub(crate) fn collect_vec(self) -> LogicPlan {
        LogicPlan::CollectVec {
            input: Box::new(self),
        }
    }

    pub(crate) fn drop(self, cols: Vec<usize>) -> LogicPlan {
        LogicPlan::DropColumns {
            input: Box::new(self),
            columns: cols,
        }
    }

    pub(crate) fn join<E: AsRef<[Expr]>>(
        self,
        other: LogicPlan,
        left_on: E,
        right_on: E,
        join_type: JoinType,
    ) -> LogicPlan {
        LogicPlan::Join {
            input_left: Box::new(self),
            input_right: Box::new(other),
            left_on: left_on.as_ref().to_vec(),
            right_on: right_on.as_ref().to_vec(),
            join_type,
        }
    }

    fn extract_header_final(schema: &Option<Schema>, projections: &Option<Vec<usize>>) -> Schema {
        match (schema, projections) {
            (Some(schema), Some(projections)) => Schema {
                columns: projections
                    .clone()
                    .into_iter()
                    .map(|i| schema.columns[i])
                    .collect(),
            },
            (Some(schema), None) => schema.clone(),
            _ => panic!(
                "Schema not found. You should set the schema as first operation after the source."
            ),
        }
    }

    pub(crate) fn schema(&self) -> Schema {
        match self {
            LogicPlan::TableScan {
                schema,
                projections,
                ..
            } => Self::extract_header_final(schema, projections),
            LogicPlan::Filter { input, .. } => input.schema(),
            LogicPlan::Select { input, .. } => input.schema(),
            LogicPlan::Shuffle { input } => input.schema(),
            LogicPlan::GroupBy { input, .. } => input.schema(),
            LogicPlan::DropKey { input } => input.schema(),
            LogicPlan::CollectVec { input } => input.schema(),
            LogicPlan::DropColumns { input, .. } => input.schema(),
            LogicPlan::Join {
                input_left,
                input_right,
                ..
            } => input_left.schema().merge(input_right.schema()),
        }
    }
}
