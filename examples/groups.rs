use renoir::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let (config, args) = RuntimeConfig::from_args();
    let context = StreamContext::new(config);

    if args.len() < 3 {
        panic!("Usage: cargo run --example groups -- [configuration parameters] <input_topic> <output_topic> <broker 1> <broker 2> ...");
    }
    let mut info = args.into_iter();
    info.next(); // skip the first argument
    let input_topic = info.next().unwrap();
    let output_topic = info.next().unwrap();
    let brokers: Vec<String> = info.collect();

    context
        .initial_group("laptops")
        .stream_from_kafka::<i32>(brokers.clone(), input_topic, Some(10))
        .filter(|x| x % 2 == 0)
        .change_group("servers")
        .window_all(CountWindow::new(10, 5, false))
        .max()
        .unkey()
        .collect_into_kakfa(output_topic, brokers);

    context.execute_blocking();

    Ok(())
}
