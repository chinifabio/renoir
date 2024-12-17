use rand::Rng;
use std::thread;
use std::time::Duration;

use renoir::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let (config, _args) = RuntimeConfig::from_args();
    log::debug!("this is the config: {config:?}");
    let context = StreamContext::new(config);

    context
        .initial_group("laptops")
        .stream_par_iter(Generator {})
        .filter(|x| x % 2 == 0)
        .change_group("servers")
        .window_all(ProcessingTimeWindow::sliding(
            Duration::from_secs(10),
            Duration::from_secs(2),
        ))
        .max()
        .unkey()
        .for_each(|x| log::info!("received: {x:?}"));

    context.execute_blocking();

    Ok(())
}

#[derive(Debug, Clone)]
struct Generator;

impl Iterator for Generator {
    type Item = i32;

    fn next(&mut self) -> Option<Self::Item> {
        let mut rng = rand::thread_rng();
        let rnd_secs = rng.gen_range(5..20);
        thread::sleep(Duration::from_secs(rnd_secs));
        Some(rng.gen_range(0..100))
    }
}

impl IntoParallelSource for Generator {
    type Iter = Generator;

    fn generate_iterator(self, index: renoir::CoordUInt, peers: renoir::CoordUInt) -> Self::Iter {
        let _ = peers;
        let _ = index;
        Generator {}
    }
}
