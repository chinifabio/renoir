use core::panic;

use itertools::Itertools;

use crate::{
    box_op::BoxedOperator,
    data_type::{NoirData, NoirType},
    operator::sink::StreamOutput,
    optimization::dsl::expressions::Expr,
    KeyedStream, Stream,
};

pub(crate) enum StreamType {
    Stream(Stream<BoxedOperator<NoirData>>),
    KeyedStream(KeyedStream<BoxedOperator<(NoirType, NoirData)>>),
    StreamOutput(StreamOutput<Vec<NoirData>>),
}

impl StreamType {
    pub(crate) fn filter_expr(self, predicate: Expr) -> Self {
        match self {
            StreamType::Stream(stream) => {
                StreamType::Stream(stream.filter_expr(predicate).into_box())
            }
            StreamType::KeyedStream(stream) => {
                StreamType::KeyedStream(stream.filter_expr(predicate))
            }
            StreamType::StreamOutput(_) => panic!("Cannot filter a stream output"),
        }
    }

    pub(crate) fn shuffle(self) -> Self {
        match self {
            StreamType::Stream(stream) => StreamType::Stream(stream.shuffle().into_box()),
            StreamType::KeyedStream(stream) => {
                StreamType::KeyedStream(stream.shuffle().to_keyed().into_box())
            }
            StreamType::StreamOutput(_) => panic!("Cannot shuffle a stream output"),
        }
    }

    pub(crate) fn group_by_expr(self, key: Expr) -> Self {
        match self {
            StreamType::Stream(stream) => {
                StreamType::KeyedStream(stream.group_by_expr(key).into_box())
            }
            StreamType::KeyedStream(_) => {
                panic!("Cannot group by a keyed stream")
            }
            StreamType::StreamOutput(_) => {
                panic!("Cannot group by a stream output")
            }
        }
    }

    pub(crate) fn drop_key(self) -> Self {
        match self {
            StreamType::Stream(_) => {
                panic!("Cannot drop key on a stream")
            }
            StreamType::KeyedStream(stream) => StreamType::Stream(stream.drop_key().into_box()),
            StreamType::StreamOutput(_) => {
                panic!("Cannot drop key a stream output")
            }
        }
    }

    pub(crate) fn select(self, columns: Vec<Expr>) -> Self {
        match self {
            StreamType::Stream(stream) => StreamType::Stream(
                stream
                    .map(move |item| {
                        let new_item_content = columns
                            .iter()
                            .map(|expr| expr.evaluate(&item))
                            .collect_vec();
                        if new_item_content.len() == 1 {
                            NoirData::NoirType(new_item_content.into_iter().next().unwrap())
                        } else {
                            NoirData::Row(new_item_content)
                        }
                    })
                    .into_box(),
            ),
            StreamType::KeyedStream(stream) => StreamType::KeyedStream(
                stream
                    .map(move |item| {
                        let new_item_content = columns
                            .iter()
                            .map(|expr| expr.evaluate(&item.1))
                            .collect_vec();
                        if new_item_content.len() == 1 {
                            NoirData::NoirType(new_item_content.into_iter().next().unwrap())
                        } else {
                            NoirData::Row(new_item_content)
                        }
                    })
                    .into_box(),
            ),
            StreamType::StreamOutput(_) => {
                panic!("Cannot select on a stream output")
            }
        }
    }

    pub(crate) fn drop_columns(self, clone: Vec<usize>) -> Self {
        match self {
            StreamType::Stream(stream) => StreamType::Stream(stream.drop_columns(clone).into_box()),
            StreamType::KeyedStream(_) => {
                // StreamType::KeyedStream {
                //     stream: stream.drop_columns(clone),
                // }
                todo!("drop columns on keyed stream")
            }
            StreamType::StreamOutput(_) => {
                panic!("Cannot drop columns on a stream output")
            }
        }
    }

    pub(crate) fn collect_vec(self) -> Self {
        match self {
            StreamType::Stream(stream) => StreamType::StreamOutput(stream.collect_vec()),
            StreamType::KeyedStream(stream) => {
                StreamType::StreamOutput(stream.drop_key().collect_vec())
            }
            StreamType::StreamOutput(_) => {
                panic!("Cannot collect a stream output")
            }
        }
    }

    pub(crate) fn into_output(self) -> StreamOutput<Vec<NoirData>> {
        match self {
            StreamType::Stream(_) => {
                panic!("Cannot convert a stream to a stream output")
            }
            StreamType::KeyedStream(_) => {
                panic!("Cannot convert a keyed stream to a stream output")
            }
            StreamType::StreamOutput(stream) => stream,
        }
    }
}
