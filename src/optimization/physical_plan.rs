use core::panic;
use std::{fmt::Display, sync::Arc};

use itertools::Itertools;
use parking_lot::Mutex;

use crate::data_type::noir_data::NoirData;
use crate::data_type::noir_type::NoirType;
use crate::data_type::schema::Schema;
use crate::data_type::stream_item::StreamItem;
use crate::operator::source::{CsvOptions, ParallelIteratorSource};
use crate::{
    box_op::BoxedOperator,
    environment::StreamEnvironmentInner,
    operator::{sink::StreamOutput, source::RowCsvSource},
    optimization::dsl::expressions::Expr,
    Stream,
};

use super::dsl::expressions::AggregateOp;
use super::logical_plan::{JoinType, LogicPlan};

#[allow(clippy::type_complexity)]
pub(crate) enum StreamType {
    Stream(Stream<BoxedOperator<StreamItem>>),
    KeyedStream(Stream<BoxedOperator<StreamItem>>),
    StreamOutput(StreamOutput<Vec<StreamItem>>),
}

impl Display for StreamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamType::Stream(_) => write!(f, "Stream"),
            StreamType::KeyedStream(_) => write!(f, "KeyedStream"),
            StreamType::StreamOutput(_) => write!(f, "StreamOutput"),
        }
    }
}

pub(crate) fn to_stream(
    logic_plan: LogicPlan,
    env: Arc<Mutex<StreamEnvironmentInner>>,
    csv_options: CsvOptions,
) -> StreamType {
    match logic_plan {
        LogicPlan::TableScan {
            path,
            predicate,
            projections,
            schema,
        } => {
            let source = RowCsvSource::new(path)
                .with_options(csv_options)
                .filter_at_source(predicate)
                .project_at_source(projections)
                .with_schema(schema);
            let stream = StreamEnvironmentInner::stream(env, source)
                .map(StreamItem::from)
                .into_box();
            StreamType::Stream(stream)
        }
        LogicPlan::Filter { predicate, input } => {
            to_stream(*input, env, csv_options).filter_expr(predicate)
        }
        LogicPlan::Select { columns, input } => to_stream(*input, env, csv_options).select(columns),
        LogicPlan::Shuffle { input } => to_stream(*input, env, csv_options).shuffle(),
        LogicPlan::GroupBy { key, input } => to_stream(*input, env, csv_options).group_by_expr(key),
        LogicPlan::DropKey { input } => to_stream(*input, env, csv_options).drop_key(),
        LogicPlan::CollectVec { input } => to_stream(*input, env, csv_options).collect_vec(),
        LogicPlan::DropColumns { input, columns } => {
            to_stream(*input, env, csv_options).drop_columns(columns)
        }
        LogicPlan::Join {
            input_left,
            input_right,
            left_on,
            right_on,
            join_type,
        } => match join_type {
            JoinType::Inner => {
                let rhs = to_stream(*input_right, env.clone(), csv_options.clone());
                to_stream(*input_left, env, csv_options).inner_join(rhs, left_on, right_on)
            }
            JoinType::Left => {
                let schema = input_left.get_schema().merge(input_right.get_schema());
                let rhs = to_stream(*input_right, env.clone(), csv_options.clone());
                to_stream(*input_left, env, csv_options).left_join(rhs, left_on, right_on, schema)
            }
            JoinType::Outer => {
                let schema = input_left.get_schema().merge(input_right.get_schema());
                let rhs = to_stream(*input_right, env.clone(), csv_options.clone());
                to_stream(*input_left, env, csv_options).outer_join(rhs, left_on, right_on, schema)
            }
        },
        LogicPlan::ParallelIterator {
            generator,
            schema: _,
        } => {
            let source = ParallelIteratorSource::new(move |i, n| {
                let boxed_iter = generator(i, n);
                boxed_iter.into_iter()
            });
            let stream = StreamEnvironmentInner::stream(env, source).into_box();
            StreamType::Stream(stream)
        }
    }
}

impl StreamType {
    pub(crate) fn filter_expr(self, predicate: Expr) -> Self {
        match self {
            StreamType::Stream(stream) => {
                StreamType::Stream(stream.filter_expr(predicate).into_box())
            }
            StreamType::KeyedStream(stream) => {
                StreamType::KeyedStream(stream.filter_expr(predicate).into_box())
            }
            _ => panic!("Cannot filter on a {}", self),
        }
    }

    pub(crate) fn shuffle(self) -> Self {
        match self {
            StreamType::Stream(stream) => StreamType::Stream(stream.shuffle().into_box()),
            StreamType::KeyedStream(stream) => StreamType::KeyedStream(stream.shuffle().into_box()),
            _ => panic!("Cannot shuffle on a {}", self),
        }
    }

    pub(crate) fn group_by_expr(self, key: Vec<Expr>) -> Self {
        match self {
            StreamType::Stream(stream) => StreamType::KeyedStream(stream.group_by_expr(key)),
            _ => panic!("Cannot group by on a {}", self),
        }
    }

    pub(crate) fn drop_key(self) -> Self {
        match self {
            StreamType::KeyedStream(stream) => {
                StreamType::Stream(stream.map(|i| i.drop_key()).into_box())
            }
            _ => panic!("Cannot drop key on a {}", self),
        }
    }

    pub(crate) fn select(self, columns: Vec<Expr>) -> Self {
        let projections = columns.clone();
        match self {
            StreamType::Stream(stream) => {
                let temp_stream = stream.map(move |item| {
                    projections
                        .iter()
                        .map(|expr| expr.evaluate(item.get_value()))
                        .collect_vec()
                });
                if columns.iter().any(|e| e.is_aggregator()) {
                    let accumulator = columns
                        .into_iter()
                        .map(|e| e.into_accumulator_state())
                        .collect_vec();
                    let stream = temp_stream
                        .fold(accumulator, |acc, value: Vec<NoirType>| {
                            for i in 0..acc.len() {
                                acc[i].accumulate(value[i]);
                            }
                        })
                        .map(|acc| acc.into_iter().map(|a| a.finalize()).collect_vec())
                        .map(StreamItem::from)
                        .into_box();
                    StreamType::Stream(stream)
                } else {
                    StreamType::Stream(temp_stream.map(StreamItem::from).into_box())
                }
            }
            StreamType::KeyedStream(stream) => {
                let temp_stream = stream.map(move |item| {
                    let temp: Vec<NoirType> = projections
                        .iter()
                        .map(|expr| expr.evaluate(item.get_value()))
                        .collect();
                    StreamItem::from(temp).absorb_key(item.get_key().unwrap().to_vec())
                });
                if columns.iter().any(|e| e.is_aggregator()) {
                    let accumulator = columns
                        .into_iter()
                        .map(|e| e.into_accumulator_state())
                        .collect_vec();
                    let stream = temp_stream
                        .group_by_fold(
                            |item| item.get_key().unwrap().to_vec(),
                            accumulator,
                            |acc: &mut Vec<AggregateOp>, value: StreamItem| {
                                for i in 0..acc.len() {
                                    acc[i].accumulate(value[i]);
                                }
                            },
                            |acc, _| {
                                for _ in 0..acc.len() {
                                    // todo: merge the accumulators
                                }
                            },
                        )
                        .map(|(_, data)| data.into_iter().map(|item| item.finalize()).collect_vec())
                        .0
                        .map(|(mut key, data)| {
                            let key_len = key.len();
                            key.extend(data);
                            StreamItem::new_with_key(key, key_len)
                        })
                        .into_box();
                    StreamType::KeyedStream(stream)
                } else {
                    StreamType::KeyedStream(temp_stream.into_box())
                }
            }
            _ => panic!("Cannot select on a {}", self),
        }
    }

    pub(crate) fn drop_columns(self, _clone: Vec<usize>) -> Self {
        match self {
            StreamType::Stream(_) => todo!(), // StreamType::Stream(stream.drop_columns(clone).into_box()),
            StreamType::KeyedStream(_) => todo!(),
            _ => panic!("Cannot drop columns on a {}", self),
        }
    }

    pub(crate) fn collect_vec(self) -> Self {
        match self {
            StreamType::Stream(stream) => StreamType::StreamOutput(stream.collect_vec()),
            StreamType::KeyedStream(stream) => StreamType::StreamOutput(stream.collect_vec()),
            _ => panic!("Cannot collect vec on a {}", self),
        }
    }

    fn keyer(item: &StreamItem, conditions: &[Expr]) -> Vec<NoirType> {
        conditions
            .iter()
            .map(|expr| expr.evaluate(item.get_value()))
            .collect()
    }

    pub(crate) fn inner_join(self, rhs: Self, left_on: Vec<Expr>, right_on: Vec<Expr>) -> Self {
        match (self, rhs) {
            (StreamType::Stream(lhs), StreamType::Stream(rhs)) => {
                let stream = lhs
                    .join(
                        rhs,
                        move |item| Self::keyer(item, &left_on),
                        move |item| Self::keyer(item, &right_on),
                    )
                    .0
                    .map(StreamItem::from)
                    .into_box();
                StreamType::KeyedStream(stream)
            }
            (lhs, rhs) => panic!("Cannot join {} with {}", lhs, rhs),
        }
    }

    pub(crate) fn left_join(
        self,
        rhs: Self,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        schema: Schema,
    ) -> Self {
        match (self, rhs) {
            (StreamType::Stream(lhs), StreamType::Stream(rhs)) => {
                let item_len = schema.columns.len();
                let stream = lhs
                    .left_join(
                        rhs,
                        move |item| Self::keyer(item, &left_on),
                        move |item| Self::keyer(item, &right_on),
                    )
                    .0
                    .map(move |item| unwrap_left_join(item, item_len))
                    .into_box();
                StreamType::KeyedStream(stream)
            }
            (lhs, rhs) => panic!("Cannot join {} with {}", lhs, rhs),
        }
    }

    pub(crate) fn outer_join(
        self,
        rhs: Self,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        schema: Schema,
    ) -> Self {
        match (self, rhs) {
            (StreamType::Stream(lhs), StreamType::Stream(rhs)) => {
                let item_len = schema.columns.len();
                let stream = lhs
                    .outer_join(
                        rhs,
                        move |item| Self::keyer(item, &left_on),
                        move |item| Self::keyer(item, &right_on),
                    )
                    .0
                    .map(move |item| unwrap_outer_join(item, item_len))
                    .into_box();
                StreamType::KeyedStream(stream)
            }
            (lhs, rhs) => panic!("Cannot join {} with {}", lhs, rhs),
        }
    }

    pub(crate) fn into_output(self) -> StreamOutput<Vec<StreamItem>> {
        match self {
            StreamType::StreamOutput(stream) => stream,
            _ => panic!("Cannot convert {} into a stream output", self),
        }
    }
}

/// Unwrap a left join, if the right side is None then fill the row with (item_len - left.len()) Nones
///
/// # Arguments
///
/// * `item` - The item to unwrap
/// * `item_len` - The length of the item
fn unwrap_left_join(
    item: (Vec<NoirType>, (StreamItem, Option<StreamItem>)),
    item_len: usize,
) -> StreamItem {
    let left = item.1 .0;
    match item.1 .1 {
        Some(right) => StreamItem::from((item.0, (left, right))),
        None => {
            let right = NoirData::Row((left.len()..item_len).map(|_| NoirType::None()).collect());
            StreamItem::from((item.0, (left, right.into())))
        }
    }
}

/// Unwrap a outer join, if the right side is None then fill the row with (item_len - left.len()) Nones.
/// If the left side is None then fill the row with (item_len - right.len()) Nones.
///
/// # Arguments
///
/// * `item` - The item to unwrap
/// * `item_len` - The length of the item
fn unwrap_outer_join(
    item: (Vec<NoirType>, (Option<StreamItem>, Option<StreamItem>)),
    item_len: usize,
) -> StreamItem {
    match (item.1 .0, item.1 .1) {
        (Some(left), Some(right)) => StreamItem::from((item.0, (left, right))),
        (Some(left), None) => {
            let right = NoirData::Row((left.len()..item_len).map(|_| NoirType::None()).collect());
            StreamItem::from((item.0, (left, right.into())))
        }
        (None, Some(right)) => {
            let left = NoirData::Row((right.len()..item_len).map(|_| NoirType::None()).collect());
            StreamItem::from((item.0, (left.into(), right)))
        }
        (None, None) => {
            let left = NoirData::Row((0..item_len).map(|_| NoirType::None()).collect());
            StreamItem::from(left)
        }
    }
}