// use std::time::Duration;

use clap::Parser;
use renoir::prelude::*;

#[derive(Parser)]
struct Args {
    n: u64,
}

fn collatz_seq(n: u64) -> u64 {
    let mut n = n;
    let mut steps = 0;
    while n > 1 {
        if n % 2 == 0 {
            n = n / 2;
        } else {
            n = 3 * n + 1;
        }
        steps += 1;
    }
    steps
}

fn main() {
    env_logger::init();

    let (config, args) = RuntimeConfig::from_args();
    config.spawn_remote_workers();

    let args = Args::parse_from(args);

    let ctx = StreamContext::new(config);
    let result = ctx
        .update_layer("edge")
        .stream_par_iter(0..args.n)
        .filter(|i| i % 3 == 0)
        .update_layer("site")
        .group_by(|e| e % 10)
        .window(CountWindow::tumbling(10))
        .sum()
        .drop_key()
        .update_layer("cloud")
        .update_requirements(s("gpu").eq("yes").and(s("memory").ge(16)))
        .filter_map(|e: u64| {
            let n = collatz_seq(e);
            if n < 100 {
                Some((e, n))
            } else {
                None
            }
        })
        .update_requirements(none())
        .collect_all::<Vec<_>>();

    ctx.execute_blocking();

    log::info!("Result: {:?}", result.get());
}