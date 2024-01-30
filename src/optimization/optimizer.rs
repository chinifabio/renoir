use super::{
    logical_plan::LogicPlan, predicate_pushdown::PredicatePushdown,
    projection_pushdown::ProjectionPushdown,
};

#[derive(Debug)]
pub(crate) enum OptimizerError {
    NotImplemented { message: String },
}

impl std::fmt::Display for OptimizerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OptimizerError::NotImplemented { message } => {
                write!(f, "Not implemented: {}", message)
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
    expression_rewrite: bool,
}

impl Default for OptimizationOptions {
    fn default() -> Self {
        Self {
            projection_pushdown: true,
            predicate_pushdown: true,
            expression_rewrite: false,
        }
    }
}

impl LogicPlanOptimizer {
    pub fn optimize(plan: LogicPlan) -> Result<LogicPlan, OptimizerError> {
        Self::optmize_with_options(plan, OptimizationOptions::default())
    }

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

        Ok(plan)
    }
}
