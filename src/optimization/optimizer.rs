use super::{logical_plan::LogicPlan, predicate_pushdown::PredicatePushdown};

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

// TODO: creare type `OptimizerResult = (LogicPlan, bool)` dove bool indica se è stato fatto un cambiamento
// creare un trait `Optimizer` con un metodo `optimize` che ritorna `OptimizerResult`
// implementare il trait per `ProjectionPushdown` e `PredicatePushdown`
// creare un ordine logico di ottimizzazioni e loopare fino a che non ci sono più cambiamenti
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

        if options.predicate_pushdown {
            plan = PredicatePushdown::new(&plan).optimize()?;
        }

        Ok(plan)
    }
}
