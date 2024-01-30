use core::panic;
use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use itertools::Itertools;
use parking_lot::Mutex;

use crate::{
    box_op::BoxedOperator,
    data_type::{NoirData, NoirType, Schema},
    environment::StreamEnvironmentInner,
    operator::{sink::StreamOutput, source::RowCsvSource, ExchangeData},
    optimization::dsl::expressions::Expr,
    stream::KeyedItem,
    KeyedStream, Stream,
};

use super::logical_plan::{JoinType, LogicPlan};

// creare i vary type per CsvRow e poi aggiungere sopra il derive serialize e poi seguire https://github.com/dtolnay/typetag
pub trait CsvRow: KeyedItem + ExchangeData + Debug {
    // TODO: change this to return option
    fn get(&self, index: usize) -> NoirType;
}

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

impl CsvRow for NoirData {
    fn get(&self, index: usize) -> NoirType {
        self[index]
    }
}

impl CsvRow for (NoirType, NoirData) {
    fn get(&self, index: usize) -> NoirType {
        self.1[index]
    }
}

impl CsvRow for (NoirData, (NoirData, NoirData)) {
    fn get(&self, index: usize) -> NoirType {
        let left_len = self.1 .0.len();
        if index < left_len {
            self.1 .0[index]
        } else {
            self.1 .1[index - left_len]
        }
    }
}

impl CsvRow for (NoirData, (NoirData, Option<NoirData>)) {
    fn get(&self, index: usize) -> NoirType {
        let left_len = self.1 .0.len();
        if index < left_len {
            self.1 .0[index]
        } else {
            self.1 .1.as_ref().unwrap()[index - left_len]
        }
    }
}

impl CsvRow for (NoirData, (Option<NoirData>, Option<NoirData>)) {
    fn get(&self, index: usize) -> NoirType {
        let left_len = self.1 .0.as_ref().unwrap().len();
        if index < left_len {
            self.1 .0.as_ref().unwrap()[index]
        } else {
            self.1 .1.as_ref().unwrap()[index - left_len]
        }
    }
}

#[allow(clippy::type_complexity)]
pub(crate) enum StreamType {
    Stream(Stream<BoxedOperator<NoirData>>),
    KeyedStream(KeyedStream<BoxedOperator<(NoirType, NoirData)>>),
    InnerJoinStream(KeyedStream<BoxedOperator<(NoirData, (NoirData, NoirData))>>),
    LeftJoinStream(KeyedStream<BoxedOperator<(NoirData, (NoirData, Option<NoirData>))>>),
    OuterJoinStream(KeyedStream<BoxedOperator<(NoirData, (Option<NoirData>, Option<NoirData>))>>),
    StreamOutput(StreamOutput<Vec<NoirData>>),
}

impl Display for StreamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamType::Stream(_) => write!(f, "Stream"),
            StreamType::KeyedStream(_) => write!(f, "KeyedStream"),
            StreamType::InnerJoinStream(_) => write!(f, "InnerJoinStream"),
            StreamType::LeftJoinStream(_) => write!(f, "LeftJoinStream"),
            StreamType::OuterJoinStream(_) => write!(f, "OuterJoinStream"),
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
        LogicPlan::CollectVec { input } => {
            let schema = input.schema();
            to_stream(*input, env).collect_vec(schema)
        }
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
                let rhs = to_stream(*input_right, env.clone());
                to_stream(*input_left, env).left_join(rhs, left_on, right_on)
            }
            JoinType::Outer => {
                let rhs = to_stream(*input_right, env.clone());
                to_stream(*input_left, env).outer_join(rhs, left_on, right_on)
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
            StreamType::InnerJoinStream(stream) => {
                StreamType::InnerJoinStream(stream.filter_expr(predicate).into_box())
            }
            StreamType::LeftJoinStream(stream) => {
                StreamType::LeftJoinStream(stream.filter_expr(predicate).into_box())
            }
            StreamType::OuterJoinStream(stream) => {
                StreamType::OuterJoinStream(stream.filter_expr(predicate).into_box())
            }
            _ => panic!("Cannot filter on a {}", self),
        }
    }

    pub(crate) fn shuffle(self) -> Self {
        match self {
            StreamType::Stream(stream) => StreamType::Stream(stream.shuffle().into_box()),
            StreamType::KeyedStream(stream) => {
                StreamType::KeyedStream(stream.shuffle().to_keyed().into_box())
            }
            StreamType::InnerJoinStream(stream) => {
                StreamType::InnerJoinStream(stream.shuffle().to_keyed().into_box())
            }
            StreamType::LeftJoinStream(stream) => {
                StreamType::LeftJoinStream(stream.shuffle().to_keyed().into_box())
            }
            StreamType::OuterJoinStream(stream) => {
                StreamType::OuterJoinStream(stream.shuffle().to_keyed().into_box())
            }
            _ => panic!("Cannot shuffle on a {}", self),
        }
    }

    pub(crate) fn group_by_expr(self, key: Expr) -> Self {
        match self {
            StreamType::Stream(stream) => StreamType::KeyedStream(
                stream.group_by_expr::<(NoirType, NoirData)>(key).into_box(),
            ),
            _ => panic!("Cannot group by on a {}", self),
        }
    }

    pub(crate) fn drop_key(self) -> Self {
        match self {
            StreamType::KeyedStream(stream) => StreamType::Stream(stream.drop_key().into_box()),
            StreamType::InnerJoinStream(_) => todo!(),
            StreamType::LeftJoinStream(_) => todo!(),
            StreamType::OuterJoinStream(_) => todo!(),
            _ => panic!("Cannot drop key on a {}", self),
        }
    }

    pub(crate) fn select(self, columns: Vec<Expr>) -> Self {
        match self {
            StreamType::Stream(stream) => {
                let stream = stream.map(move |item| {
                    let new_item_content = columns
                        .iter()
                        .map(|expr| expr.evaluate(&item))
                        .collect_vec();
                    if new_item_content.len() == 1 {
                        NoirData::NoirType(new_item_content.into_iter().next().unwrap())
                    } else {
                        NoirData::Row(new_item_content)
                    }
                });
                StreamType::Stream(stream.into_box())
            }
            StreamType::KeyedStream(stream) => {
                let stream = stream.map(move |item| {
                    let new_item_content = columns
                        .iter()
                        .map(|expr| expr.evaluate(&item.1))
                        .collect_vec();
                    if new_item_content.len() == 1 {
                        NoirData::NoirType(new_item_content.into_iter().next().unwrap())
                    } else {
                        NoirData::Row(new_item_content)
                    }
                });
                StreamType::KeyedStream(stream.into_box())
            }
            StreamType::InnerJoinStream(_) => todo!(),
            StreamType::LeftJoinStream(_) => todo!(),
            StreamType::OuterJoinStream(_) => todo!(),
            _ => panic!("Cannot select on a {}", self),
        }
    }

    pub(crate) fn drop_columns(self, clone: Vec<usize>) -> Self {
        match self {
            StreamType::Stream(stream) => StreamType::Stream(stream.drop_columns(clone).into_box()),
            StreamType::KeyedStream(_) => todo!(),
            StreamType::InnerJoinStream(_) => todo!(),
            StreamType::LeftJoinStream(_) => todo!(),
            StreamType::OuterJoinStream(_) => todo!(),
            _ => panic!("Cannot drop columns on a {}", self),
        }
    }

    pub(crate) fn collect_vec(self, schema: Schema) -> Self {
        match self {
            StreamType::Stream(stream) => StreamType::StreamOutput(stream.collect_vec()),
            StreamType::KeyedStream(stream) => {
                StreamType::StreamOutput(stream.drop_key().collect_vec())
            }
            StreamType::InnerJoinStream(stream) => {
                StreamType::StreamOutput(stream.unkey().map(unwrap_inner_join).collect_vec())
            }
            StreamType::LeftJoinStream(stream) => {
                let item_len = schema.columns.len();
                StreamType::StreamOutput(
                    stream
                        .unkey()
                        .map(move |item| unwrap_left_join(item, item_len))
                        .collect_vec(),
                )
            }
            StreamType::OuterJoinStream(stream) => {
                let item_len = schema.columns.len();
                StreamType::StreamOutput(
                    stream
                        .unkey()
                        .map(move |item| unwrap_outer_join(item, item_len))
                        .collect_vec(),
                )
            }
            _ => panic!("Cannot collect vec on a {}", self),
        }
    }

    fn keyer(item: &NoirData, conditions: &[Expr]) -> NoirData {
        let data = conditions
            .iter()
            .map(|expr| expr.evaluate(item))
            .collect_vec();
        if data.len() == 1 {
            NoirData::NoirType(data.into_iter().next().unwrap())
        } else {
            NoirData::Row(data)
        }
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
                    .into_box();
                StreamType::InnerJoinStream(stream)
            }
            (lhs, rhs) => panic!("Cannot join {} with {}", lhs, rhs),
        }
    }

    pub(crate) fn left_join(self, rhs: Self, left_on: Vec<Expr>, right_on: Vec<Expr>) -> Self {
        match (self, rhs) {
            (StreamType::Stream(lhs), StreamType::Stream(rhs)) => {
                let stream = lhs
                    .left_join(
                        rhs,
                        move |item| Self::keyer(item, &left_on),
                        move |item| Self::keyer(item, &right_on),
                    )
                    .into_box();
                StreamType::LeftJoinStream(stream)
            }
            (lhs, rhs) => panic!("Cannot join {} with {}", lhs, rhs),
        }
    }

    pub(crate) fn outer_join(self, rhs: Self, left_on: Vec<Expr>, right_on: Vec<Expr>) -> Self {
        match (self, rhs) {
            (StreamType::Stream(lhs), StreamType::Stream(rhs)) => {
                let stream = lhs
                    .outer_join(
                        rhs,
                        move |item| Self::keyer(item, &left_on),
                        move |item| Self::keyer(item, &right_on),
                    )
                    .into_box();
                StreamType::OuterJoinStream(stream)
            }
            (lhs, rhs) => panic!("Cannot join {} with {}", lhs, rhs),
        }
    }

    pub(crate) fn into_output(self) -> StreamOutput<Vec<NoirData>> {
        match self {
            StreamType::StreamOutput(stream) => stream,
            _ => panic!("Cannot convert {} into a stream output", self),
        }
    }
}

fn unwrap_inner_join(item: (NoirData, (NoirData, NoirData))) -> NoirData {
    let left = item.1 .0;
    let right = item.1 .1;
    let mut data = Vec::with_capacity(left.len() + right.len());
    data.extend(left.row());
    data.extend(right.row());
    NoirData::Row(data)
}

fn unwrap_left_join(item: (NoirData, (NoirData, Option<NoirData>)), item_len: usize) -> NoirData {
    let left = item.1 .0;
    match item.1 .1 {
        Some(right) => {
            let mut data = Vec::with_capacity(left.len() + right.len());
            data.extend(left.row());
            data.extend(right.row());
            NoirData::Row(data)
        }
        None => {
            let mut data = Vec::with_capacity(item_len);
            data.extend(left.row());
            for _ in left.len()..item_len {
                data.push(NoirType::None());
            }
            NoirData::Row(data)
        }
    }
}

fn unwrap_outer_join(
    item: (NoirData, (Option<NoirData>, Option<NoirData>)),
    item_len: usize,
) -> NoirData {
    match (item.1 .0, item.1 .1) {
        (Some(left), Some(right)) => {
            let mut data = Vec::with_capacity(left.len() + right.len());
            data.extend(left.row());
            data.extend(right.row());
            NoirData::Row(data)
        }
        (Some(left), None) => {
            let mut data = Vec::with_capacity(left.len());
            data.extend(left.row());
            for _ in left.len()..item_len {
                data.push(NoirType::None());
            }
            NoirData::Row(data)
        }
        (None, Some(right)) => {
            let mut data = Vec::with_capacity(right.len());
            for _ in right.len()..item_len {
                data.push(NoirType::None());
            }
            data.extend(right.row());
            NoirData::Row(data)
        }
        (None, None) => NoirData::NoirType(NoirType::None()),
    }
}
