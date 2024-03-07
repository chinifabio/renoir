use super::{
    expression_compile::ExpressionCompile, logical_plan::LogicPlan,
    predicate_pushdown::PredicatePushdown, projection_pushdown::ProjectionPushdown,
};

#[derive(Debug)]
pub(crate) enum OptimizerError {
    NotImplemented { message: String },
    SchemaNotDefined { message: String },
}

impl std::fmt::Display for OptimizerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OptimizerError::NotImplemented { message } => {
                write!(f, "Not implemented: {}", message)
            }
            OptimizerError::SchemaNotDefined { message } => {
                write!(f, "Missing schema: {}", message)
            }
        }
    }
}

pub(crate) type OptimizerResult = Result<LogicPlan, OptimizerError>;

pub(crate) trait OptimizationRule {
    fn optimize(plan: LogicPlan) -> OptimizerResult;
}

pub(crate) struct LogicPlanOptimizer {}

pub struct OptimizationOptions {
    projection_pushdown: bool,
    predicate_pushdown: bool,
    compile_expressions: bool,
    expression_rewrite: bool,
}

impl Default for OptimizationOptions {
    fn default() -> Self {
        Self {
            projection_pushdown: true,
            predicate_pushdown: true,
            compile_expressions: false,
            expression_rewrite: false,
        }
    }
}

impl LogicPlanOptimizer {
    pub fn optmize_with_options(
        mut plan: LogicPlan,
        options: OptimizationOptions,
    ) -> Result<LogicPlan, OptimizerError> {
        if options.projection_pushdown {
            plan = ProjectionPushdown::optimize(plan)?;
        }

        if options.predicate_pushdown {
            plan = PredicatePushdown::optimize(plan)?;
        }

        if options.expression_rewrite {
            todo!()
        }

        if options.compile_expressions {
            plan = ExpressionCompile::optimize(plan)?;
        }

        Ok(plan)
    }
}

impl OptimizationOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_projection_pushdown(self, enable: bool) -> Self {
        Self {
            projection_pushdown: enable,
            ..self
        }
    }

    pub fn with_predicate_pushdown(self, enable: bool) -> Self {
        Self {
            predicate_pushdown: enable,
            ..self
        }
    }

    pub fn with_compile_expressions(self, enable: bool) -> Self {
        Self {
            compile_expressions: enable,
            ..self
        }
    }

    pub fn with_expression_rewrite(self, enable: bool) -> Self {
        Self {
            expression_rewrite: enable,
            ..self
        }
    }

    pub fn none() -> OptimizationOptions {
        OptimizationOptions {
            projection_pushdown: false,
            predicate_pushdown: false,
            compile_expressions: false,
            expression_rewrite: false,
        }
    }
}
