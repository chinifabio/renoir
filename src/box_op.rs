use std::{fmt::Display, marker::PhantomData};

use dyn_clone::DynClone;

use crate::{
    block::BlockStructure,
    data_type::StreamItem,
    operator::{Data, Operator, StreamElement},
    ExecutionMetadata, KeyedStream, Stream,
};

pub(crate) trait DynOperator<T: Data>: DynClone {
    /// Setup the operator chain. This is called before any call to `next` and it's used to
    /// initialize the operator. When it's called the operator has already been cloned and it will
    /// never be cloned again. Therefore it's safe to store replica-specific metadata inside of it.
    ///
    /// It's important that each operator (except the start of a chain) calls `.setup()` recursively
    /// on the previous operators.
    fn setup(&mut self, metadata: &mut ExecutionMetadata);

    /// Take a value from the previous operator, process it and return it.
    fn next(&mut self) -> StreamElement<T>;

    /// A more refined representation of the operator and its predecessors.
    fn structure(&self) -> BlockStructure;
}

dyn_clone::clone_trait_object!(<T> DynOperator<T>);

impl<T: Data, O: Operator<Out = T>> DynOperator<T> for O {
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.setup(metadata)
    }

    fn next(&mut self) -> StreamElement<T> {
        self.next()
    }

    fn structure(&self) -> BlockStructure {
        self.structure()
    }
}

pub struct BoxedOperator<T> {
    pub(crate) op: Box<dyn DynOperator<T> + 'static + Send>,
    pub(crate) _t: PhantomData<T>,
}

impl<T> Clone for BoxedOperator<T> {
    fn clone(&self) -> Self {
        Self {
            op: self.op.clone(),
            _t: PhantomData,
        }
    }
}

impl<T> Display for BoxedOperator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BoxedOperator")
    }
}

impl<T: Data> BoxedOperator<T> {
    pub fn new<O: Operator<Out = T> + 'static>(op: O) -> Self {
        Self {
            op: Box::new(op),
            _t: PhantomData,
        }
    }
}

impl<T: Data> Operator for BoxedOperator<T> {
    type Out = T;

    fn next(&mut self) -> StreamElement<T> {
        self.op.next()
    }

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.op.setup(metadata)
    }

    fn structure(&self) -> BlockStructure {
        self.op.structure()
    }
}

impl<Op> Stream<Op>
where
    Op: Operator<Out = StreamItem> + 'static,
{
    pub fn into_box(self) -> Stream<BoxedOperator<Op::Out>> {
        self.add_operator(|prev| BoxedOperator::new(prev))
    }
}

impl<Op> KeyedStream<Op>
where
    Op: Operator<Out = StreamItem> + 'static,
{
    pub fn into_box(self) -> KeyedStream<BoxedOperator<Op::Out>> {
        self.add_operator(|prev| BoxedOperator::new(prev))
    }
}
