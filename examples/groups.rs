use renoir::prelude::*;

fn main() {
    env_logger::init();

    // TODO: add a parameter to define the distributed runtime
    let config = RuntimeConfig::distributed().expect("Failed to create config");
    let context = StreamContext::new(config);

    let brokers = vec!["localhost:9093".to_string()];

    context
        .start_tier("yoga")
        .stream_from_kafka::<i32>(brokers.clone(), "input", Some(10))
        .filter(|x| x % 2 == 0)
        .change_tier("sola")
        .map(|x| x % 10)
        .collect_into_kakfa("output", brokers);

    context.execute_blocking();
}
