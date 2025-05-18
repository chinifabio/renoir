use crate::{block::NextStrategy, operator::{end::End, ExchangeData, Operator}, Stream};

impl<Op> Stream<Op> 
where
    Op: Operator + 'static,
    Op::Out: ExchangeData,
{
    pub fn update_layer(self, layer: impl Into<String>) -> Stream<impl Operator<Out = Op::Out>> {
        self.ctx.lock().update_layer(layer);
        self.split_block(End::new, NextStrategy::random())
    }
}