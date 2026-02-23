use std::time::Duration;

use renoir::{operator::Timestamp, prelude::*};

#[derive(Clone)]
struct StatefulMapper {
    state: i32,
}

impl CheckpointedFn<(), i32, i32> for StatefulMapper {
    type State = i32;

    fn process(&mut self, input: i32) -> i32 {
        self.state += input;
        self.state
    }

    fn snapshot(&self) -> Self::State {
        self.state
    }

    fn restore(&mut self, snapshot: Self::State) {
        self.state = snapshot;
    }
}

fn main() {
    env_logger::init();

    let (config, _) = RuntimeConfig::from_args();
    config.spawn_remote_workers();
    let ctx = StreamContext::new(config);

    ctx.stream_par_iter(0..100)
        .map(|e| {
            std::thread::sleep(Duration::from_secs(10));
            e
        })
        .add_timestamps(
            |&n| n as Timestamp,
            |&n, &ts| if n % 2 == 0 { Some(ts) } else { None },
        )
        .rich_map_resilient(StatefulMapper { state: 0 })
        .filter(|i| i % 10 == 0)
        .for_each(|i| log::info!("Received: {i}"));

    ctx.with_checkpoint_manager().execute_blocking();
}
