use renoir::prelude::*;

struct InMemoryStorage;

impl StorageBackend for InMemoryStorage {
    fn name(&self) -> &str {
        "InMemoryStorage"
    }

    fn save(&self, key: &str, data: bytes::Bytes) {
        println!("Saving checkpoint for {}: {} bytes", key, data.len());
    }

    fn load(&self, key: &str) -> bytes::Bytes {
        println!("Loading checkpoint for {}", key);
        bytes::Bytes::new()
    }

    fn delete(&self, key: &str) {
        println!("Deleting checkpoint for {}", key);
    }
}

fn main() {
    let ctx = StreamContext::new_local();

    let checkpoint_manager = CheckpointManager::new(Box::new(InMemoryStorage));

    ctx.stream_par_iter(0..100)
        .for_each(|i| println!("Received: {i}"));

    ctx.with_checkpoint_manager(checkpoint_manager)
        .execute_blocking();
}
