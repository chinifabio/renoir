use clap::Parser;
use renoir::prelude::*;

#[derive(Parser)]
struct Args {
    n: u64,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (config, args) = RuntimeConfig::from_args();
    config.spawn_remote_workers();

    let args = Args::parse_from(args);

    let ctx = StreamContext::new(config);
    let result = ctx
        .initial_layer("edge")
        .stream_par_iter(0..args.n)
        .filter(|i| i % 2 == 0)
        .change_layer("site")
        .group_by_avg(move |e| e  % 100, |e| *e as f64)
        .drop_key()
        .change_layer("cloud")
        .map(|e| e * e)
        .collect_all::<Vec<_>>();

    ctx.execute().await;

    log::info!("Result: {:?}", result.get());
}
