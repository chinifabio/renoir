use core::panic;
use std::{fmt::Display, sync::Arc};

use itertools::Itertools;
use parking_lot::Mutex;

use crate::{
    block::NextStrategy,
    box_op::BoxedOperator,
    data_type::{NoirData, NoirType, Schema, StreamItem},
    environment::StreamEnvironmentInner,
    operator::{end::End, sink::StreamOutput, source::RowCsvSource},
    optimization::dsl::expressions::Expr,
    stream::KeyedItem,
    KeyedStream, Stream,
};

use super::logical_plan::{JoinType, LogicPlan};

impl KeyedItem for NoirData {
    type Key = NoirData;

    type Value = NoirData;

    fn key(&self) -> &Self::Key {
        self
    }

    fn value(&self) -> &Self::Value {
        self
    }

    fn into_kv(self) -> (Self::Key, Self::Value) {
        (self.clone(), self)
    }
}

#[allow(clippy::type_complexity)]
pub(crate) enum StreamType {
    Stream(Stream<BoxedOperator<StreamItem>>),
    KeyedStream(KeyedStream<BoxedOperator<StreamItem>>),
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
) -> StreamType {
    match logic_plan {
        LogicPlan::TableScan {
            path,
            predicate,
            projections,
            schema,
        } => {
            let source = RowCsvSource::new(path)
                .filter_at_source(predicate)
                .project_at_source(projections)
                .with_schema(schema);
            let stream = StreamEnvironmentInner::stream(env, source).into_box();
            StreamType::Stream(stream)
        }
        LogicPlan::Filter { predicate, input } => to_stream(*input, env).filter_expr(predicate),
        LogicPlan::Select { columns, input } => to_stream(*input, env).select(columns),
        LogicPlan::Shuffle { input } => to_stream(*input, env).shuffle(),
        LogicPlan::GroupBy { key, input } => to_stream(*input, env).group_by_expr(key),
        LogicPlan::DropKey { input } => to_stream(*input, env).drop_key(),
        LogicPlan::CollectVec { input } => to_stream(*input, env).collect_vec(),
        LogicPlan::DropColumns { input, columns } => to_stream(*input, env).drop_columns(columns),
        LogicPlan::Join {
            input_left,
            input_right,
            left_on,
            right_on,
            join_type,
        } => match join_type {
            JoinType::Inner => {
                let rhs = to_stream(*input_right, env.clone());
                to_stream(*input_left, env).inner_join(rhs, left_on, right_on)
            }
            JoinType::Left => {
                let schema = input_left.schema().merge(input_right.schema());
                let rhs = to_stream(*input_right, env.clone());
                to_stream(*input_left, env).left_join(rhs, left_on, right_on, schema)
            }
            JoinType::Outer => {
                let schema = input_left.schema().merge(input_right.schema());
                let rhs = to_stream(*input_right, env.clone());
                to_stream(*input_left, env).outer_join(rhs, left_on, right_on, schema)
            }
        },
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
            StreamType::KeyedStream(stream) => StreamType::KeyedStream(
                stream
                    .0
                    .split_block(End::new, NextStrategy::random())
                    .to_keyed()
                    .into_box(),
            ),
            _ => panic!("Cannot shuffle on a {}", self),
        }
    }

    pub(crate) fn group_by_expr(self, key: Vec<Expr>) -> Self {
        match self {
            StreamType::Stream(stream) => {
                StreamType::KeyedStream(stream.group_by_expr(key).into_box())
            }
            _ => panic!("Cannot group by on a {}", self),
        }
    }

    pub(crate) fn drop_key(self) -> Self {
        match self {
            StreamType::KeyedStream(stream) => {
                StreamType::Stream(stream.0.map(|i| i.drop_key()).into_box())
            }
            _ => panic!("Cannot drop key on a {}", self),
        }
    }

    pub(crate) fn select(self, columns: Vec<Expr>) -> Self {
        let mapping = move |item| {
            let new_item_content = columns
                .iter()
                .map(|expr| expr.evaluate(&item))
                .collect_vec();
            if new_item_content.len() == 1 {
                StreamItem::DataItem(NoirData::NoirType(
                    new_item_content.into_iter().next().unwrap(),
                ))
            } else {
                StreamItem::DataItem(NoirData::Row(new_item_content))
            }
        };
        match self {
            StreamType::Stream(stream) => {
                let stream = stream.map(mapping).into_box();
                StreamType::Stream(stream)
            }
            StreamType::KeyedStream(stream) => {
                let stream = stream.0.map(mapping).into_box();
                StreamType::Stream(stream)
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
            StreamType::KeyedStream(stream) => StreamType::StreamOutput(stream.0.collect_vec()),
            _ => panic!("Cannot collect vec on a {}", self),
        }
    }

    fn keyer(item: &StreamItem, conditions: &[Expr]) -> Vec<NoirType> {
        conditions.iter().map(|expr| expr.evaluate(item)).collect()
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
                    .to_keyed()
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
                    .to_keyed()
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
                    .to_keyed()
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
            let right = NoirData::Row(
                (left.value().len()..item_len)
                    .map(|_| NoirType::None())
                    .collect(),
            );
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
            let right = NoirData::Row(
                (left.value().len()..item_len)
                    .map(|_| NoirType::None())
                    .collect(),
            );
            StreamItem::from((item.0, (left, right.into())))
        }
        (None, Some(right)) => {
            let left = NoirData::Row(
                (right.value().len()..item_len)
                    .map(|_| NoirType::None())
                    .collect(),
            );
            StreamItem::from((item.0, (left.into(), right)))
        }
        (None, None) => {
            let left = NoirData::Row((0..item_len).map(|_| NoirType::None()).collect());
            StreamItem::from(left)
        }
    }
}
