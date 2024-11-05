use renoir::prelude::*;

fn main() {
    env_logger::init();

    let config = RuntimeConfig::remote("groups.toml").expect("config error");
    config.spawn_remote_workers();

    let context = StreamContext::new(config);

    let result_a = context
        .start_tier("group_a")
        .stream_iter(0..100)
        .group_by(|x| x % 10)
        .filter(|(_, v)| v % 2 == 0)
        .connect_direct_group("group_b")
        .fold(0, |acc, value| *acc += value)
        .collect_vec();

    let result_b = context
        .start_tier("group_a")
        .stream_iter(0..100)
        .shuffle()
        .filter(|x| x % 2 == 0)
        .connect_group("group_b")
        .group_by_count(|x| x % 10)
        .collect_vec();

    context.execute_blocking();

    println!("A: {:?}", result_a.get().unwrap_or_default());
    println!("B: {:?}", result_b.get().unwrap_or_default());
}
