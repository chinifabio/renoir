use crate::operator::{ExchangeData, StreamElement};

use super::MessageMetadata;

#[derive(Debug, Clone, Default)]
pub(crate) struct LayoutFrontier {
    
}

impl LayoutFrontier {
    pub(crate) fn update<T: ExchangeData>(&self, metadata: MessageMetadata, item: &StreamElement<T>) -> Option<StreamElement<T>> {
        todo!()
    }
}