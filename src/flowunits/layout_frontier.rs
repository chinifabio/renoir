use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::operator::{StreamElement, Timestamp};

use super::MessageMetadata;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
enum ElementVariant {
    Watermark(Timestamp),
    FlushBatch,
    FlushAndRestart,
    Terminate,
}

impl<T> From<&StreamElement<T>> for ElementVariant {
    fn from(value: &StreamElement<T>) -> Self {
        match value {
            StreamElement::Watermark(ts) => ElementVariant::Watermark(*ts),
            StreamElement::FlushBatch => ElementVariant::FlushBatch,
            StreamElement::FlushAndRestart => ElementVariant::FlushAndRestart,
            StreamElement::Terminate => ElementVariant::Terminate,
            _ => panic!("Unexpected StreamElement variant"),
        }
    }
}

#[derive(Debug, Clone)]
/// Non uso arc perche' ho un frontier per ogni worker. così mi evito il lock e il 'broadcast'
pub(crate) struct LayoutFrontier {
    connected: HashMap<MessageMetadata, Instant>,
    received: HashMap<ElementVariant, usize>,
}

impl LayoutFrontier {
    pub fn new() -> Self {
        Self {
            connected: HashMap::new(),
            received: HashMap::new(),
        }
    }

    /// Tracks broadcasted messages
    ///
    /// Waits to receive all the messages from the same layer and replica and then send the message into the current layer
    pub(crate) fn update<T: Clone>(
        &mut self,
        metadata: MessageMetadata,
        item: &StreamElement<T>,
        now: Instant,
    ) -> Option<StreamElement<T>> {
        log::debug!(
            "Updating LayoutFrontier with metadata: {:?}, item: {}",
            metadata,
            item.variant_str()
        );

        if !self.connected.contains_key(&metadata) {
            log::info!(
                "New metadata detected: {:?}. Adding to connected.",
                metadata
            );
            self.connected.insert(metadata.clone(), now);
        }

        let counter = self.received.entry(item.into()).or_insert(0);
        *counter += 1;
        log::debug!(
            "Updated received count for item {}: {}",
            item.variant_str(),
            *counter
        );

        let parallelism = self
            .connected
            .keys()
            .filter_map(|m| {
                if m.layer == metadata.layer {
                    Some(m.parallelism)
                } else {
                    None
                }
            })
            .sum::<u64>();
        log::debug!(
            "Calculated parallelism for layer {}: {}",
            metadata.layer,
            parallelism
        );

        if *counter == parallelism as usize {
            log::info!(
                "All messages received for item {} in layer {}. Removing from received.",
                item.variant_str(),
                metadata.layer
            );
            self.received.remove(&item.into());
            return Some(item.clone());
        }

        log::debug!(
            "Not all messages received for item {}. Current count: {}, required: {}",
            item.variant_str(),
            *counter,
            parallelism
        );

        None
    }

    /// Remove from the connected groups all the one that are outdated
    pub(crate) fn timed_out(&mut self, now: Instant, interval: Duration) {
        log::warn!("Cleaning up connected groups");
        self.connected
            .retain(|_, v| now.duration_since(*v) < interval);
    }

    pub(crate) fn heartbeat(&mut self, now: Instant, metadata: &MessageMetadata) {
        log::debug!("Heartbeat for {:?}", metadata);
        self.connected.insert(metadata.clone(), now);
    }
}
