use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    ClientConfig,
};
use redis::*;
use renoir::operator::source::connectors::kafka::KafkaSourceConnector;
use renoir::operator::source::connectors::redis::RedisSourceConnector;
use renoir::prelude::*;

#[test]
fn redis_connection() {
    let redis = Client::open("redis://127.0.0.1").expect("Cannot connect to redis");
    let redis_key = "cacchina";

    let config = RuntimeConfig::default();
    config.spawn_remote_workers();
    let env = StreamContext::new(config);
    let n = 10000;

    env.stream_iter(0..n)
        .shuffle()
        .filter(|x| x % 2 == 0)
        .redis_sink(redis_key, redis.clone());

    let result = env
        .stream_connector::<i32, _>(RedisSourceConnector::new(redis, redis_key))
        .group_by_count(|x| x % 10)
        .collect_vec();

    env.execute_blocking();

    let data = result.get().unwrap();
    assert_eq!(data.len(), 5);
    assert!(data
        .iter()
        .all(|(key, value)| key % 2 == 0 && *value == 1000));
}

#[test]
fn kafka_connection() {
    let hosts = vec![
        "localhost:9091".to_string(),
        "localhost:9092".to_string(),
        "localhost:9093".to_string(),
    ];
    let topic = "test";

    let config = RuntimeConfig::default();
    let env = StreamContext::new(config);
    let n = 10000;

    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", hosts.join(","))
        .create()
        .expect("Kafka consumer creation failed");
    for i in 0..8 {
        let final_topic = format!("{}-{}", topic, i);
        let new_topic = [NewTopic::new(
            final_topic.as_str(),
            1,
            TopicReplication::Fixed(1),
        )];
        let _future = admin.create_topics(&new_topic, &AdminOptions::default());
    }
    std::thread::sleep(std::time::Duration::from_secs(1));

    env.stream_iter(0..n)
        .shuffle()
        .filter(|x| x % 2 == 0)
        .kafka_sink(topic, hosts.clone());

    let result = env
        .stream_connector::<i32, _>(KafkaSourceConnector::new(hosts, topic))
        .group_by_count(|x| x % 10)
        .collect_vec();

    env.execute_blocking();

    let data = result.get().unwrap();
    assert_eq!(data.len(), 5);
    assert!(data
        .iter()
        .all(|(key, value)| key % 2 == 0 && *value == 1000));
}
