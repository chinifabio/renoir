use clap::Parser;
use rdkafka::{ClientConfig, Message};
use renoir::prelude::*;

#[derive(Parser)]
struct Args {
    input_topic: String,
    output_topic: String,
    broker: String,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (config, args) = RuntimeConfig::from_args();
    config.spawn_remote_workers();

    let args = Args::parse_from(args);

    let mut client_config = ClientConfig::new();
    client_config
        .set("group.id", "renoir-flowunits")
        .set("bootstrap.servers", args.broker.as_str())
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest");
    let mut producer_config = ClientConfig::new();
    producer_config
        .set("bootstrap.servers", args.broker.as_str())
        .set("message.timeout.ms", "5000");

    let mut time = 0;
    let ctx = StreamContext::new(config);
    ctx.initial_layer("edge")
        .stream_kafka(
            client_config,
            &[args.input_topic.as_str()],
            Replication::Unlimited,
        )
        .map(|m| {
            let payload = m.payload().expect("Failed to get payload");
            String::from_utf8_lossy(payload)
                .parse::<u64>()
                .expect("Failed to parse payload")
        })
        .add_timestamps(
            move |_| {
                time += 1;
                time
            },
            move |_, t| {
                if t % 10 == 0 {
                    Some(*t)
                } else {
                    None
                }
            },
        )
        .filter(|i| i % 2 == 0)
        .change_layer("site")
        .window_all(CountWindow::tumbling(10))
        .sum()
        .drop_key()
        .map(|s: u64| (s / 10))
        .change_layer("cloud")
        .map(|i: u64| i.to_string())
        .write_kafka(producer_config, args.output_topic.as_str());

    ctx.execute().await;
}
