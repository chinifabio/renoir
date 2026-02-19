use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{RuntimeConfig, network::Coord};

pub trait StorageBackend {
    fn from_config(config: Arc<RuntimeConfig>) -> Self;
    fn name(&self) -> &str;
    fn save<I: Serialize>(&self, key: &str, data: &I);
    fn load<O: for<'de> Deserialize<'de>>(&self, key: &str) -> Option<O>;
    fn delete(&self, key: &str);
}

pub type ActiveStorage = InMemoryStorage;

pub type CheckpointManagerRef = Arc<Mutex<CheckpointManager>>;

pub struct CheckpointManager {
    storage: ActiveStorage,
}

impl std::fmt::Debug for CheckpointManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointManager")
            .field("storage", &self.storage.name())
            .finish()
    }
}

impl CheckpointManager {
    pub fn from(config: Arc<RuntimeConfig>) -> Self {
        CheckpointManager {
            storage: ActiveStorage::from_config(config),
        }
    }

    pub fn checkpoint<I: Serialize>(&mut self, coord: Coord, state: &I) {
        self.storage.save(&coord.to_string(), state);
    }

    pub fn restore<O: for<'de> Deserialize<'de>>(&mut self, coord: Coord) -> Option<O> {
        self.storage.load(&coord.to_string())
    }
}

pub struct InMemoryStorage;

impl StorageBackend for InMemoryStorage {
    fn from_config(_config: Arc<RuntimeConfig>) -> Self {
        Self
    }

    fn name(&self) -> &str {
        "InMemoryStorage"
    }

    fn save<I: Serialize>(&self, key: &str, data: &I) {
        let serialized = serde_json::to_string(data).expect("Failed to serialize checkpoint");
        println!("Saving checkpoint for {}: {}", key, serialized);
    }

    fn load<O: for<'de> Deserialize<'de>>(&self, key: &str) -> Option<O> {
        println!("Loading checkpoint for {}", key);
        None // No checkpoint available
    }

    fn delete(&self, key: &str) {
        println!("Deleting checkpoint for {}", key);
    }
}
