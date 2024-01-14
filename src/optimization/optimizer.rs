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
    fn optimize(&self) -> OptimizerResult;
}

pub(crate) struct LogicPlanOptimizer {
    plan: LogicPlan,
}

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
            expression_rewrite: true,
        }
    }
}

impl LogicPlanOptimizer {
    pub fn new(plan: LogicPlan) -> Self {
        Self { plan }
    }

    pub fn optimize(self) -> Result<LogicPlan, OptimizerError> {
        self.optmize_with_options(OptimizationOptions::default())
    }

    pub fn optmize_with_options(
        self,
        options: OptimizationOptions,
    ) -> Result<LogicPlan, OptimizerError> {
        let mut plan = self.plan;

        if options.projection_pushdown {
            plan = ProjectionPushdown::new(&plan).optimize()?;
        }

        if options.predicate_pushdown {
            plan = PredicatePushdown::new(&plan).optimize()?;
        }

        Ok(plan)
    }
}
