use core::panic;
use std::fmt::Debug;
use std::fmt::Display;
use std::path::PathBuf;

use crate::box_op::BoxedOperator;
use crate::data_type::schema::Schema;
use crate::data_type::stream_item::StreamItem;
use crate::Stream;

use super::dsl::expressions::*;
use super::optimizer::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Outer,
}

pub enum LogicPlan {
    UpStream {
        stream: Stream<BoxedOperator<StreamItem>>,
        schema: Option<Schema>,
    },
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
    Mean {
        input: Box<LogicPlan>,
        skip_na: bool,
    },
    GroupbySelect {
        input: Box<LogicPlan>,
        keys: Vec<Expr>,
        aggs: Vec<Expr>,
    },
}

impl Clone for LogicPlan {
    fn clone(&self) -> Self {
        match self {
            Self::UpStream { .. } => panic!("UpStream cannot be cloned."),
            Self::TableScan {
                path,
                predicate,
                projections,
                schema,
            } => Self::TableScan {
                path: path.clone(),
                predicate: predicate.clone(),
                projections: projections.clone(),
                schema: schema.clone(),
            },
            Self::Filter { predicate, input } => Self::Filter {
                predicate: predicate.clone(),
                input: input.clone(),
            },
            Self::Select { columns, input } => Self::Select {
                columns: columns.clone(),
                input: input.clone(),
            },
            Self::Shuffle { input } => Self::Shuffle {
                input: input.clone(),
            },
            Self::GroupBy { key, input } => Self::GroupBy {
                key: key.clone(),
                input: input.clone(),
            },
            Self::DropKey { input } => Self::DropKey {
                input: input.clone(),
            },
            Self::CollectVec { input } => Self::CollectVec {
                input: input.clone(),
            },
            Self::DropColumns { input, columns } => Self::DropColumns {
                input: input.clone(),
                columns: columns.clone(),
            },
            Self::Join {
                input_left,
                input_right,
                left_on,
                right_on,
                join_type,
            } => Self::Join {
                input_left: input_left.clone(),
                input_right: input_right.clone(),
                left_on: left_on.clone(),
                right_on: right_on.clone(),
                join_type: join_type.clone(),
            },
            Self::Mean { input, skip_na } => Self::Mean {
                input: input.clone(),
                skip_na: skip_na.clone(),
            },
            Self::GroupbySelect { input, keys, aggs } => Self::GroupbySelect {
                input: input.clone(),
                keys: keys.clone(),
                aggs: aggs.clone(),
            },
        }
    }
}

impl Eq for LogicPlan {}

impl PartialEq for LogicPlan {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::UpStream {
                    schema: l_schema, ..
                },
                Self::UpStream {
                    schema: r_schema, ..
                },
            ) => l_schema == r_schema,
            (
                Self::TableScan {
                    path: l_path,
                    predicate: l_predicate,
                    projections: l_projections,
                    schema: l_schema,
                },
                Self::TableScan {
                    path: r_path,
                    predicate: r_predicate,
                    projections: r_projections,
                    schema: r_schema,
                },
            ) => {
                l_path == r_path
                    && l_predicate == r_predicate
                    && l_projections == r_projections
                    && l_schema == r_schema
            }
            (
                Self::Filter {
                    predicate: l_predicate,
                    input: l_input,
                },
                Self::Filter {
                    predicate: r_predicate,
                    input: r_input,
                },
            ) => l_predicate == r_predicate && l_input == r_input,
            (
                Self::Select {
                    columns: l_columns,
                    input: l_input,
                },
                Self::Select {
                    columns: r_columns,
                    input: r_input,
                },
            ) => l_columns == r_columns && l_input == r_input,
            (Self::Shuffle { input: l_input }, Self::Shuffle { input: r_input }) => {
                l_input == r_input
            }
            (
                Self::GroupBy {
                    key: l_key,
                    input: l_input,
                },
                Self::GroupBy {
                    key: r_key,
                    input: r_input,
                },
            ) => l_key == r_key && l_input == r_input,
            (Self::DropKey { input: l_input }, Self::DropKey { input: r_input }) => {
                l_input == r_input
            }
            (Self::CollectVec { input: l_input }, Self::CollectVec { input: r_input }) => {
                l_input == r_input
            }
            (
                Self::DropColumns {
                    input: l_input,
                    columns: l_columns,
                },
                Self::DropColumns {
                    input: r_input,
                    columns: r_columns,
                },
            ) => l_input == r_input && l_columns == r_columns,
            (
                Self::Join {
                    input_left: l_input_left,
                    input_right: l_input_right,
                    left_on: l_left_on,
                    right_on: l_right_on,
                    join_type: l_join_type,
                },
                Self::Join {
                    input_left: r_input_left,
                    input_right: r_input_right,
                    left_on: r_left_on,
                    right_on: r_right_on,
                    join_type: r_join_type,
                },
            ) => {
                l_input_left == r_input_left
                    && l_input_right == r_input_right
                    && l_left_on == r_left_on
                    && l_right_on == r_right_on
                    && l_join_type == r_join_type
            }
            _ => false,
        }
    }
}

impl Debug for LogicPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UpStream { schema, .. } => f
                .debug_struct("ParallelIterator")
                .field("schema", schema)
                .finish(),
            Self::TableScan {
                path,
                predicate,
                projections,
                schema,
            } => f
                .debug_struct("TableScan")
                .field("path", path)
                .field("predicate", predicate)
                .field("projections", projections)
                .field("schema", schema)
                .finish(),
            Self::Filter { predicate, input } => f
                .debug_struct("Filter")
                .field("predicate", predicate)
                .field("input", input)
                .finish(),
            Self::Select { columns, input } => f
                .debug_struct("Select")
                .field("columns", columns)
                .field("input", input)
                .finish(),
            Self::Shuffle { input } => f.debug_struct("Shuffle").field("input", input).finish(),
            Self::GroupBy { key, input } => f
                .debug_struct("GroupBy")
                .field("key", key)
                .field("input", input)
                .finish(),
            Self::DropKey { input } => f.debug_struct("DropKey").field("input", input).finish(),
            Self::CollectVec { input } => {
                f.debug_struct("CollectVec").field("input", input).finish()
            }
            Self::DropColumns { input, columns } => f
                .debug_struct("DropColumns")
                .field("input", input)
                .field("columns", columns)
                .finish(),
            Self::Join {
                input_left,
                input_right,
                left_on,
                right_on,
                join_type,
            } => f
                .debug_struct("Join")
                .field("input_left", input_left)
                .field("input_right", input_right)
                .field("left_on", left_on)
                .field("right_on", right_on)
                .field("join_type", join_type)
                .finish(),
            Self::Mean { input, skip_na } => f
                .debug_struct("Mean")
                .field("input", input)
                .field("skip_na", skip_na)
                .finish(),
            Self::GroupbySelect { input, keys, aggs } => f
                .debug_struct("GroupbyFold")
                .field("input", input)
                .field("keys", keys)
                .field("aggs", aggs)
                .finish(),
        }
    }
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
            LogicPlan::UpStream { stream: _, schema } => {
                write!(f, "UpStream({:?})", schema)
            }
            LogicPlan::Mean { input, .. } => {
                write!(f, "{} -> Mean", input)
            }
            LogicPlan::GroupbySelect { input, keys, aggs } => {
                write!(f, "{} -> GroupbyFold({:?}, {:?})", input, keys, aggs)
            }
        }
    }
}

// todo spostare metodi dentro opt stream
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

    pub(crate) fn mean(self, skip_na: bool) -> LogicPlan {
        LogicPlan::Mean {
            input: Box::new(self),
            skip_na,
        }
    }

    pub(crate) fn join<E: AsRef<[Expr]>>(
        self,
        other: LogicPlan,
        left_on: E,
        right_on: E,
        join_type: JoinType,
    ) -> LogicPlan {
        let left_vec = left_on.as_ref().to_vec();
        let right_vec = right_on.as_ref().to_vec();
        assert_eq!(
            left_vec.len(),
            right_vec.len(),
            "Left and right keys must have the same length."
        );
        LogicPlan::Join {
            input_left: Box::new(self),
            input_right: Box::new(other),
            left_on: left_vec,
            right_on: right_vec,
            join_type,
        }
    }

    pub(crate) fn set_schema(&mut self, schema: Schema) {
        match self {
            LogicPlan::TableScan { schema: s, .. } => *s = Some(schema),
            LogicPlan::UpStream { schema: s, .. } => *s = Some(schema),
            LogicPlan::Filter { input, .. } => input.set_schema(schema),
            LogicPlan::Select { input, .. } => input.set_schema(schema),
            LogicPlan::Shuffle { input } => input.set_schema(schema),
            LogicPlan::GroupBy { input, .. } => input.set_schema(schema),
            LogicPlan::DropKey { input } => input.set_schema(schema),
            LogicPlan::CollectVec { input } => input.set_schema(schema),
            LogicPlan::DropColumns { input, .. } => input.set_schema(schema),
            LogicPlan::Join { .. } => panic!("Schema should be set before the join operation."),
            LogicPlan::Mean { input, .. } => input.set_schema(schema),
            LogicPlan::GroupbySelect { input, .. } => input.set_schema(schema),
        }
    }

    pub(crate) fn get_schema(&self) -> Schema {
        match self {
            LogicPlan::TableScan {
                schema,
                projections,
                ..
            } => match (schema, projections) {
                (Some(schema), Some(projections)) => Schema {
                    columns: projections
                        .clone()
                        .into_iter()
                        .map(|i| schema.columns[i])
                        .collect(),
                },
                (Some(schema), None) => schema.clone(),
                _ => panic!("Schema not found. You should set the schema."),
            },
            LogicPlan::Filter { input, .. } => input.get_schema(),
            LogicPlan::Select { input, .. } => input.get_schema(),
            LogicPlan::Shuffle { input } => input.get_schema(),
            LogicPlan::GroupBy { input, .. } => input.get_schema(),
            LogicPlan::DropKey { input } => input.get_schema(),
            LogicPlan::CollectVec { input } => input.get_schema(),
            LogicPlan::DropColumns { input, .. } => input.get_schema(),
            LogicPlan::Join {
                input_left,
                input_right,
                ..
            } => input_left.get_schema().merge(input_right.get_schema()),
            LogicPlan::UpStream { stream: _, schema } => schema.clone().expect(
                "Schema not found. You should set the schema after the conversion to OptStream.",
            ),
            LogicPlan::Mean { input, .. } => input.get_schema(),
            LogicPlan::GroupbySelect { input, .. } => input.get_schema(),
        }
    }

    pub(crate) fn infer_schema(&mut self) {
        match self {
            LogicPlan::TableScan { path, schema, .. } => {
                *schema = Some(Schema::infer_from_file(path.clone()))
            }
            LogicPlan::Filter { input, .. } => input.infer_schema(),
            LogicPlan::Select { input, .. } => input.infer_schema(),
            LogicPlan::Shuffle { input } => input.infer_schema(),
            LogicPlan::GroupBy { input, .. } => input.infer_schema(),
            LogicPlan::DropKey { input } => input.infer_schema(),
            LogicPlan::CollectVec { input } => input.infer_schema(),
            LogicPlan::DropColumns { input, .. } => input.infer_schema(),
            LogicPlan::Join {
                input_left,
                input_right,
                ..
            } => {
                input_left.infer_schema();
                input_right.infer_schema();
            }
            LogicPlan::UpStream { .. } => {
                panic!("Cannot infer schema from UpStream, set it manually.")
            }
            LogicPlan::Mean { input, .. } => input.infer_schema(),
            LogicPlan::GroupbySelect { input, .. } => input.infer_schema(),
        }
    }
}
