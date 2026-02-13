use renoir::{operator::Timestamp, prelude::*};
use serde::{Deserialize, Serialize};

struct InMemoryStorage;

impl StorageBackend for InMemoryStorage {
    fn name(&self) -> &str {
        "InMemoryStorage"
    }

    fn save(&self, key: &str, data: Vec<u8>) {
        println!("Saving checkpoint for {}: {} bytes", key, data.len());
    }

    fn load(&self, key: &str) -> Vec<u8> {
        println!("Loading checkpoint for {}", key);
        Vec::new()
    }

    fn delete(&self, key: &str) {
        println!("Deleting checkpoint for {}", key);
    }
}

#[derive(Clone)]
struct StatefulMapper {
    state: i32,
}

impl CheckpointedFn<(), i32, i32> for StatefulMapper {
    fn process(&mut self, input: i32) -> i32 {
        self.state += input;
        self.state
    }

    fn snapshot(&self) -> Vec<u8> {
        let serialized = bincode::serde::encode_to_vec(&self.state, bincode::config::standard()).expect("Failed to serialize checkpoint state");
        serialized
    }

    fn restore(&mut self, snapshot: Vec<u8>) {
        let (state, _) = bincode::serde::decode_from_slice(&snapshot, bincode::config::standard()).expect("Failed to deserialize checkpoint state");
        self.state = state;
    }
}

fn main() {
    let ctx = StreamContext::new_local();

    let checkpoint_manager = CheckpointManager::new(Box::new(InMemoryStorage));

    ctx.stream_par_iter(0..100)
        .add_timestamps(|&n| n as Timestamp, |&n, &ts| if n % 2 == 0 { Some(ts) } else { None })
        // .rich_map_resilient({
        //     let mut state = 0;
        //     move |(_key, o): (&(), i32)| {
        //         state += 1;
        //         o + state
        //     }
        // })
        .rich_map_resilient(StatefulMapper { state: 0 })
        .for_each(|i| println!("Received: {i}"));

    ctx.with_checkpoint_manager(checkpoint_manager)
        .execute_blocking();
}
