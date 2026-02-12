use std::sync::Arc;

use parking_lot::Mutex;

use crate::network::Coord;

pub trait Checkpointable {
    fn checkpoint(&self) -> bytes::Bytes;
    fn restore(&mut self, data: &[u8]);
}

pub trait StorageBackend {
    fn name(&self) -> &str;
    fn save(&self, key: &str, data: bytes::Bytes);
    fn load(&self, key: &str) -> bytes::Bytes;
    fn delete(&self, key: &str);
}

pub type CheckpointManagerRef = Arc<Mutex<CheckpointManager>>;

pub struct CheckpointManager {
    storage: Box<dyn StorageBackend + Send + Sync>,
}

impl std::fmt::Debug for CheckpointManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointManager")
            .field("storage", &self.storage.name())
            .finish()
    }
}

impl CheckpointManager {
    pub fn new(storage: Box<dyn StorageBackend + Send + Sync>) -> Self {
        Self { storage }
    }

    pub fn checkpoint(&mut self, coord: Coord, state: impl Checkpointable) {
        let data = state.checkpoint();
        self.storage.save(&coord.to_string(), data);
    }

    pub fn restore(&mut self, coord: Coord, state: &mut impl Checkpointable) {
        let data = self.storage.load(&coord.to_string());
        state.restore(&data);
    }
}
