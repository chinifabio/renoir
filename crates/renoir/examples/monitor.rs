use rand::RngCore;
use renoir::prelude::*;

#[derive(Clone)]
struct TimerParallelGenerator {
    start: usize,
    end: usize,
}

struct TimerParallelIterator {
    start: usize,
    end: usize,
}

impl Iterator for TimerParallelIterator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            let value = self.start;
            self.start += 1;
            std::thread::sleep(std::time::Duration::from_secs(5));
            if rand::rng().next_u32() % 10 == 0 {
                panic!("Simulated failure on item {}", value);
            }
            Some(value)
        } else {
            None
        }
    }
}

impl IntoParallelSource for TimerParallelGenerator {
    type Iter = TimerParallelIterator;

    fn generate_iterator(self, index: renoir_core::CoordUInt, peers: renoir_core::CoordUInt) -> Self::Iter {
        let n = self.end - self.start;
        let chunk_size = n.div_ceil(peers as usize);
        let start = self.start.saturating_add((index as usize) * chunk_size);
        let end = (start.saturating_add(chunk_size))
            .min(self.end)
            .max(self.start);
        TimerParallelIterator { start, end }
    }
}

fn main() {
    env_logger::init();

    let ctx = StreamContext::new_local();

    ctx.stream_par_iter(TimerParallelGenerator { start: 0, end: 1000 })
        .for_each(|_x| {
            // println!("Processing item {}", x);
        });
    
    ctx.execute_blocking();
}