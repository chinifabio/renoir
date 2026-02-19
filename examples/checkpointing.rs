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
    let ctx = StreamContext::new_local();

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

    ctx.with_checkpoint_manager().execute_blocking();
}
