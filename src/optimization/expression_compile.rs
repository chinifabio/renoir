use crate::data_type::schema::Schema;

use super::{
    dsl::jit::JitCompiler,
    logical_plan::LogicPlan,
    optimizer::{OptimizationRule, OptimizerError, OptimizerResult},
};

pub(crate) struct ExpressionCompile {}

impl ExpressionCompile {
    pub fn compile(
        plan: LogicPlan,
        jit_compiler: &mut JitCompiler,
    ) -> Result<(LogicPlan, Schema), OptimizerError> {
        match plan {
            LogicPlan::TableScan {
                predicate,
                path,
                projections,
                schema,
            } => {
                if schema.is_none() {
                    return Err(OptimizerError::SchemaNotDefined { message: "You are trying to compile expression without schema, set the schema or do not compile expressions".to_owned() });
                }

                let new_schema = schema.clone().unwrap().with_projections(&projections);

                Ok((
                    LogicPlan::TableScan {
                        predicate: predicate.map(|p| p.compile(&new_schema, jit_compiler)),
                        path,
                        projections,
                        schema,
                    },
                    new_schema,
                ))
            }
            LogicPlan::Filter { predicate, input } => {
                let (new_input, schema) = Self::compile(*input, jit_compiler)?;
                Ok((
                    LogicPlan::Filter {
                        predicate: predicate.compile(&schema, jit_compiler),
                        input: Box::new(new_input),
                    },
                    schema,
                ))
            }
            LogicPlan::Select { columns, input } => {
                let (new_input, schema) = Self::compile(*input, jit_compiler)?;
                let new_schema = schema.update(&columns);
                Ok((
                    LogicPlan::Select {
                        columns: columns
                            .into_iter()
                            .map(|c| c.compile(&schema, jit_compiler))
                            .collect(),
                        input: Box::new(new_input),
                    },
                    new_schema,
                ))
            }
            LogicPlan::Shuffle { input } => {
                let (new_input, schema) = Self::compile(*input, jit_compiler)?;
                Ok((
                    LogicPlan::Shuffle {
                        input: Box::new(new_input),
                    },
                    schema,
                ))
            }
            LogicPlan::GroupBy { key, input } => {
                let (new_input, schema) = Self::compile(*input, jit_compiler)?;
                Ok((
                    LogicPlan::GroupBy {
                        key: key
                            .into_iter()
                            .map(|k| k.compile(&schema, jit_compiler))
                            .collect(),
                        input: Box::new(new_input),
                    },
                    schema,
                ))
            }
            LogicPlan::DropKey { input } => {
                let (new_input, schema) = Self::compile(*input, jit_compiler)?;
                Ok((
                    LogicPlan::DropKey {
                        input: Box::new(new_input),
                    },
                    schema,
                ))
            }
            LogicPlan::CollectVec { input } => {
                let (new_input, schema) = Self::compile(*input, jit_compiler)?;
                Ok((
                    LogicPlan::CollectVec {
                        input: Box::new(new_input),
                    },
                    schema,
                ))
            }
            LogicPlan::DropColumns { input, columns } => {
                let (new_input, schema) = Self::compile(*input, jit_compiler)?;
                Ok((
                    LogicPlan::DropColumns {
                        input: Box::new(new_input),
                        columns,
                    },
                    schema,
                ))
            }
            LogicPlan::Join {
                input_left,
                input_right,
                left_on,
                right_on,
                join_type,
            } => {
                let (new_input_left, schema_left) = Self::compile(*input_left, jit_compiler)?;
                let (new_input_right, schema_right) = Self::compile(*input_right, jit_compiler)?;

                let new_left_on = left_on
                    .into_iter()
                    .map(|k| k.compile(&schema_left, jit_compiler))
                    .collect();
                let new_right_on = right_on
                    .into_iter()
                    .map(|k| k.compile(&schema_right, jit_compiler))
                    .collect();
                let new_schema = schema_left.merge(schema_right);

                Ok((
                    LogicPlan::Join {
                        input_left: Box::new(new_input_left),
                        input_right: Box::new(new_input_right),
                        left_on: new_left_on,
                        right_on: new_right_on,
                        join_type,
                    },
                    new_schema,
                ))
            }
            LogicPlan::ParallelIterator { generator, schema } => {
                let new_schema = schema.clone();
                Ok((
                    LogicPlan::ParallelIterator { generator, schema },
                    new_schema,
                ))
            }
        }
    }
}

impl OptimizationRule for ExpressionCompile {
    fn optimize(plan: LogicPlan) -> OptimizerResult {
        match Self::compile(plan, &mut JitCompiler::default()) {
            Ok((new_plan, ..)) => Ok(new_plan),
            Err(e) => Err(e),
        }
    }
}
