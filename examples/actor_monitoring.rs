use renoir::{prelude::ParallelIteratorSource, RuntimeConfig, StreamContext};

#[tokio::main]
async fn main() {
    env_logger::init();
    println!("Actor monitoring example started");

    let (config, _args) = RuntimeConfig::from_args();
    config.spawn_remote_workers();

    let env = StreamContext::new(config);

    let source = ParallelIteratorSource::new(0..1_000_000);
    env.stream(source)
        .map(|x| x * 2)
        .for_each(|x| {
            if x % 1_000_000 == 0 {
                println!("Processed {}", x);
            }
        });

    env.execute().await;
}
