use renoir::prelude::*;

fn main() {
    env_logger::init();

    let (config, args) = RuntimeConfig::from_args();
    let context = StreamContext::new(config);

    let brokers = vec!["localhost:9093".to_string()];

    let input_topic = args.get(1).cloned().unwrap_or_default();

    context
        .start_tier("yoga")
        .stream_from_kafka::<i32>(brokers.clone(), input_topic, Some(10))
        .filter(|x| x % 2 == 0)
        .add_group_name()
        .change_tier("sola")
        .map(|(g, x)| (g, x % 10))
        .group_by(|(_, x)| x % 10)
        .unkey()
        .collect_into_kakfa("output", brokers);

    context.execute_blocking();
}
