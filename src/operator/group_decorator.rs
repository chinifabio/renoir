use std::fmt::Display;

use crate::block::OperatorStructure;

use super::Operator;

#[derive(Debug, Clone)]
pub struct GroupReplicaDecorator<Op>
where
    Op: Operator,
{
    previous: Op,
    group_name: Option<String>,
}

impl<Op> Display for GroupReplicaDecorator<Op>
where
    Op: Operator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> GroupReplicaDecorator", self.previous)
    }
}

impl<Op> GroupReplicaDecorator<Op>
where
    Op: Operator,
{
    pub fn new(previous: Op) -> Self {
        Self {
            previous,
            group_name: None,
        }
    }
}

impl<Op> Operator for GroupReplicaDecorator<Op>
where
    Op: Operator,
{
    type Out = (String, Op::Out);

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.group_name = metadata.group_name().clone();
        self.previous.setup(metadata);
    }

    fn next(&mut self) -> super::StreamElement<Self::Out> {
        let element = self.previous.next();
        let group_name = self.group_name.clone().unwrap_or_default();
        element.map(|data| (group_name, data))
    }

    fn structure(&self) -> super::BlockStructure {
        let structure = OperatorStructure::new::<Self::Out, _>("GroupDecorator");
        self.previous.structure().add_operator(structure)
    }
}
