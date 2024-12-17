use std::{collections::HashMap, sync::Arc, thread::JoinHandle, time::Instant};

use parking_lot::Mutex;
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    producer::{BaseProducer, BaseRecord},
    util::Timeout,
    ClientConfig, Message,
};
use serde::{Deserialize, Serialize};

use crate::config::DistributedConfig;
use std::thread;
use std::time::Duration;

pub(crate) mod emitter;
pub(crate) mod receiver;

pub(crate) type GroupName = String;
pub(crate) type GroupMap = Arc<Mutex<HashMap<GroupName, usize>>>;

#[derive(Debug, Serialize, Deserialize)]
enum HearbeatMessage {
    GroupJoin(GroupName, usize),
    Heartbeat(GroupName),
    GroupLeave(GroupName),
}

#[derive(Debug)]
pub struct HeartbeatManager {
    handle: Vec<JoinHandle<()>>,
    config: Arc<DistributedConfig>,
    is_running: Arc<Mutex<bool>>,
}

impl Clone for HeartbeatManager {
    fn clone(&self) -> Self {
        Self {
            handle: Vec::new(),
            config: self.config.clone(),
            is_running: self.is_running.clone(),
        }
    }
}

impl HeartbeatManager {
    pub fn new(config: Arc<DistributedConfig>) -> Self {
        HeartbeatManager {
            handle: Vec::new(),
            config,
            is_running: Arc::new(Mutex::new(false)),
        }
    }

    #[allow(dead_code)]
    pub fn stop(&mut self) {
        *self.is_running.lock() = false;
        for handle in self.handle.drain(0..) {
            handle.join().expect("Failed to join the heartbeat thread!");
        }
    }

    pub fn start_emitter(&mut self) {
        log::debug!("Starting emitter...");
        let channel = self
            .config
            .heartbeat_channel
            .clone()
            .expect("Missing heartbeat channel in distributed config");
        let group_name = match &self.config.host_group_replica {
            Some(replica) => format!("{}-{}", self.config.host_group, replica),
            None => self.config.host_group.clone(),
        };
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", channel.brokers.join(","))
            .create()
            .expect("Cannot create producer for heartbear manager");
        let interval = channel
            .timeout
            .expect("Missing timeout distributed_config.heartbeat_channel.timeout");

        let is_running = self.is_running.clone();
        *is_running.lock() = true;
        self.handle.push(thread::spawn(move || {
            log::debug!("Sending group join");
            Self::send_message(
                HearbeatMessage::GroupJoin(group_name.clone(), 999),
                &producer,
                channel.topic.as_str(),
            )
            .expect("Failed to send heartbeat group join!");

            while *is_running.lock() {
                // todo capire se serve aggiungere il numero progressivo
                log::debug!("Sending heartbeat");
                let message = HearbeatMessage::Heartbeat(group_name.clone());
                Self::send_message(message, &producer, channel.topic.as_str())
                    .expect("Failed to send heartbeat!");
                thread::sleep(Duration::from_secs(interval / 2));
            }
        }));
    }

    fn send_message(
        message: HearbeatMessage,
        producer: &BaseProducer,
        topic: &str,
    ) -> Result<(), ()> {
        let json = serde_json::to_string(&message)
            .unwrap_or_else(|_| panic!("Failed to serialize heartbeat message {:?}", message));
        let record = BaseRecord::to(topic).payload(&json).key("franca-garzotto");
        producer
            .send(record)
            .expect(format!("Failed to send heartbeat message {:?}", message).as_str());
        Ok(())
    }

    pub fn start_receiver(&mut self, groups: GroupMap, id: u64) {
        let times = Arc::new(Mutex::new(HashMap::new()));

        let channel = self
            .config
            .heartbeat_channel
            .clone()
            .expect("Missing heartbeat channel in distributed config");
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", channel.brokers.join(","))
            .set("group.id", format!("renoir-heartbeat-receiver-{id}"))
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Cannot create producer for heartbear manager");
        consumer
            .subscribe(&[channel.topic.as_str()])
            .expect("Heartbeat receiver failed to subscribe to topic");
        let interval = Duration::from_secs(
            channel
                .timeout
                .expect("Missing timeout distributed_config.heartbeat_channel.timeout"),
        );

        let kafka_groups = groups.clone();
        let kafka_times = times.clone();
        let is_running = self.is_running.clone();
        *is_running.lock() = true;
        self.handle.push(thread::spawn(move || {
            while *is_running.lock() {
                match consumer.poll(Timeout::After(interval)) {
                    Some(Ok(bmessage)) => {
                        let payload = bmessage.payload().unwrap();
                        match serde_json::from_slice(payload).unwrap() {
                            HearbeatMessage::GroupJoin(key, parallelism) => {
                                kafka_times.lock().insert(key.clone(), Instant::now());
                                kafka_groups.lock().insert(key, parallelism);
                            }
                            HearbeatMessage::Heartbeat(key) => {
                                kafka_times.lock().insert(key.clone(), Instant::now());
                                assert!(
                                    kafka_groups.lock().contains_key(&key),
                                    "Heartbeat without group join"
                                );
                            }
                            HearbeatMessage::GroupLeave(key) => {
                                kafka_times.lock().remove(&key);
                                kafka_groups.lock().remove(&key);
                            }
                        }
                    }
                    Some(Err(e)) => {
                        log::error!("Kafka failed: {e:?}");
                        panic!("Kafka failed, see logs for details.");
                    }
                    None => {
                        log::warn!("Received empty message from Kafka")
                    }
                }
            }
        }));

        let mut earlier = Instant::now();
        self.handle.push(std::thread::spawn(move || {
            let now = Instant::now();
            loop {
                let mut times_guard = times.lock();
                let mut disconnected = Vec::new();
                for (key, last_time) in times_guard.iter() {
                    if now.duration_since(*last_time) > interval {
                        log::warn!("GROUP DISCONNECTED: {key}");
                        disconnected.push(key.clone());
                        groups.lock().remove(key);
                    }
                }
                disconnected.into_iter().for_each(|k| {
                    times_guard.remove(&k);
                });
                log::debug!("Connected groups: {:?}", times.lock().keys());

                if let Some(time_to_sleep) =
                    interval.checked_sub(Instant::now().duration_since(earlier))
                {
                    std::thread::sleep(time_to_sleep);
                }
                earlier = Instant::now();
            }
        }));
    }
}
