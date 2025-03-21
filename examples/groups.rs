use renoir::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let (config, _) = RuntimeConfig::from_args();
    config.spawn_remote_workers();

    let size = std::env::var("RENOIR_TEST_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100);

    let context = StreamContext::new(config);
    let result = context
        .initial_group("edge")
        .stream_par_iter(0..size)
        .filter(|x| x % 3 == 0)
        .change_group("site")
        .group_by_avg(|x| x % 100, |x| *x as f64)
        .drop_key()
        .map(|x| x.round() as i32)
        .change_group("cloud")
        .group_by_avg(|x| x % 10, |x| *x as f64)
        .drop_key()
        .map(|x| x.round() as i32)
        .collect_all::<Vec<_>>();

    context.execute().await;

    println!("done: {:?}", result.get());

    Ok(())
}
