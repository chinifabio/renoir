use renoir::prelude::*;

fn main() {
    env_logger::init();

    let config = RuntimeConfig::remote("groups.toml").expect("config error");
    config.spawn_remote_workers();

    let context = StreamContext::new(config);

    let data = context
        .deployment_group("group_a")
        .stream_iter(0..100)
        .group_by(|x| x % 10)
        .filter(|(_, v)| v % 2 == 0)
        .deployment_group("group_b")
        .fold(0, |acc, value| *acc += value)
        .collect_vec();

    let data_keyed = context
        .deployment_group("group_a")
        .stream_iter(0..100)
        .shuffle()
        .filter(|x| x % 2 == 0)
        .deployment_group("group_b")
        .group_by_count(|x| x % 10)
        .collect_vec();

    context.execute_blocking();

    println!("Data: {:?}", data.get().unwrap_or_default());
    println!("Data keyed {:?}", data_keyed.get().unwrap_or_default());
}
