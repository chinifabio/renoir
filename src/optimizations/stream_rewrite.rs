use super::{
    logical_plan::LogicPlan,
    optimizer::{OptimizationRule, OptimizerResult},
};

pub struct StreamRewriter {}

impl StreamRewriter {
    fn rewrite(plan: LogicPlan) -> LogicPlan {
        match plan {
            LogicPlan::GroupBy {
                key,
                input: input_gb,
            } => {
                let new_input = Box::new(StreamRewriter::rewrite(*input_gb));
                LogicPlan::GroupBy {
                    key,
                    input: new_input,
                }
            }
            LogicPlan::UpStream { stream, schema } => LogicPlan::UpStream { stream, schema },
            LogicPlan::TableScan {
                path,
                predicate,
                projections,
                schema,
            } => LogicPlan::TableScan {
                path,
                predicate,
                projections,
                schema,
            },
            LogicPlan::Filter { predicate, input } => {
                let new_input = Box::new(StreamRewriter::rewrite(*input));
                LogicPlan::Filter {
                    predicate,
                    input: new_input,
                }
            }
            LogicPlan::Select {
                columns,
                input: input_s,
            } => match *input_s {
                LogicPlan::GroupBy {
                    key,
                    input: input_gb,
                } => {
                    if columns.iter().any(|c| c.is_aggregator()) {
                        let new_input = Box::new(StreamRewriter::rewrite(*input_gb));
                        LogicPlan::GroupbySelect {
                            input: new_input,
                            keys: key,
                            aggs: columns,
                        }
                    } else {
                        let new_input = Box::new(StreamRewriter::rewrite(*input_gb));
                        LogicPlan::GroupBy {
                            key,
                            input: new_input,
                        }
                    }
                }
                _ => {
                    let new_input = Box::new(StreamRewriter::rewrite(*input_s));
                    LogicPlan::Select {
                        columns,
                        input: new_input,
                    }
                }
            },
            LogicPlan::Shuffle { input } => {
                let new_input = Box::new(StreamRewriter::rewrite(*input));
                LogicPlan::Shuffle { input: new_input }
            }
            LogicPlan::DropKey { input } => {
                let new_input = Box::new(StreamRewriter::rewrite(*input));
                LogicPlan::DropKey { input: new_input }
            }
            LogicPlan::CollectVec { input } => {
                let new_input = Box::new(StreamRewriter::rewrite(*input));
                LogicPlan::CollectVec { input: new_input }
            }
            LogicPlan::DropColumns { input, columns } => {
                let new_input = Box::new(StreamRewriter::rewrite(*input));
                LogicPlan::DropColumns {
                    input: new_input,
                    columns,
                }
            }
            LogicPlan::Join {
                input_left,
                input_right,
                left_on,
                right_on,
                join_type,
            } => {
                let new_input_left = Box::new(StreamRewriter::rewrite(*input_left));
                let new_input_right = Box::new(StreamRewriter::rewrite(*input_right));
                LogicPlan::Join {
                    input_left: new_input_left,
                    input_right: new_input_right,
                    left_on,
                    right_on,
                    join_type,
                }
            }
            LogicPlan::Mean { input, skip_na } => {
                let new_input = Box::new(StreamRewriter::rewrite(*input));
                LogicPlan::Mean {
                    input: new_input,
                    skip_na,
                }
            }
            LogicPlan::GroupbySelect { input, keys, aggs } => {
                let new_input = Box::new(StreamRewriter::rewrite(*input));
                LogicPlan::GroupbySelect {
                    input: new_input,
                    keys,
                    aggs,
                }
            }
        }
    }
}

impl OptimizationRule for StreamRewriter {
    fn optimize(mut plan: LogicPlan) -> OptimizerResult {
        plan = StreamRewriter::rewrite(plan);
        Ok(plan)
    }
}
