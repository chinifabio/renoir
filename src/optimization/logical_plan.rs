use std::fmt::Debug;
use std::fmt::Display;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::environment::StreamEnvironmentInner;
use crate::operator::source::RowCsvSource;

use super::dsl::expressions::*;
use super::optimizer::*;
use super::stream_wrapper::StreamType;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogicPlan {
    TableScan {
        path: PathBuf,
        predicate: Option<Expr>,
        projections: Option<Vec<usize>>,
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
        key: Expr,
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
}

impl Display for LogicPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicPlan::TableScan {
                path,
                predicate,
                projections,
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
                write!(f, "{} -> GroupBy({})", input, key)
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
        }
    }
}

impl LogicPlan {
    pub(crate) fn optimize(self) -> LogicPlan {
        match LogicPlanOptimizer::new(self).optimize() {
            Ok(plan) => {
                // println!("Optimized plan: {}", plan);
                plan
            }
            Err(err) => panic!("Error during optimization -> {}", err),
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

    pub(crate) fn group_by(self, key: Expr) -> LogicPlan {
        LogicPlan::GroupBy {
            key,
            input: Box::new(self),
        }
    }

    pub(crate) fn drop_key(self) -> LogicPlan {
        LogicPlan::DropKey {
            input: Box::new(self),
        }
    }

    pub(crate) fn select(self, columns: &[Expr]) -> LogicPlan {
        LogicPlan::Select {
            columns: columns.to_vec(),
            input: Box::new(self),
        }
    }

    pub(crate) fn collect_vec(self) -> LogicPlan {
        LogicPlan::CollectVec {
            input: Box::new(self),
        }
    }

    pub(crate) fn drop(&self, cols: Vec<usize>) -> LogicPlan {
        LogicPlan::DropColumns {
            input: Box::new(self.clone()),
            columns: cols,
        }
    }

    pub(crate) fn to_stream(&self, env: Arc<Mutex<StreamEnvironmentInner>>) -> StreamType {
        match self {
            LogicPlan::TableScan {
                path,
                predicate,
                projections,
            } => {
                let source = RowCsvSource::new(path)
                    .filter_at_source(predicate.clone())
                    .project_at_source(projections.clone());
                let stream = StreamEnvironmentInner::stream(env, source).into_box();
                StreamType::Stream(stream)
            }
            LogicPlan::Filter { predicate, input } => {
                input.to_stream(env).filter_expr(predicate.clone())
            }
            LogicPlan::Select { columns, input } => input.to_stream(env).select(columns.clone()),
            LogicPlan::Shuffle { input } => input.to_stream(env).shuffle(),
            LogicPlan::GroupBy { key, input } => input.to_stream(env).group_by_expr(key.clone()),
            LogicPlan::DropKey { input } => input.to_stream(env).drop_key(),
            LogicPlan::CollectVec { input } => input.to_stream(env).collect_vec(),
            LogicPlan::DropColumns { input, columns } => {
                input.to_stream(env).drop_columns(columns.clone())
            }
        }
    }
}
