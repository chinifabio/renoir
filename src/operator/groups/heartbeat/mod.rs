use std::{collections::HashMap, sync::Arc, thread::JoinHandle, time::Instant};

use parking_lot::Mutex;
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    producer::{BaseProducer, BaseRecord},
    util::Timeout,
    ClientConfig, Message,
};
use serde::{Deserialize, Serialize};

use crate::{config::DistributedConfig, operator::Timestamp, CoordUInt};
use std::thread;
use std::time::Duration;

use super::{GroupMap, GroupName};

pub(crate) mod emitter;
pub(crate) mod receiver;

#[derive(Debug, Serialize, Deserialize)]
struct Heartbeat(GroupName, CoordUInt);

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

    pub fn start_emitter(&mut self, group_parallelism: CoordUInt) {
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
        let interval = Duration::from_secs(
            channel
                .timeout
                .expect("Missing timeout distributed_config.heartbeat_channel.timeout"),
        );

        let is_running = self.is_running.clone();
        *is_running.lock() = true;
        self.handle.push(thread::spawn(move || {
            log::debug!("Sending group join");
            Self::send_message(
                Heartbeat(group_name.clone(), group_parallelism),
                &producer,
                channel.topic.as_str(),
            )
            .expect("Failed to send heartbeat group join!");

            while *is_running.lock() {
                // todo capire se serve aggiungere il numero progressivo
                log::debug!("Sending heartbeat");
                let message = Heartbeat(group_name.clone(), group_parallelism);
                Self::send_message(message, &producer, channel.topic.as_str())
                    .expect("Failed to send heartbeat!");
                thread::sleep(interval);
            }
        }));
    }

    fn send_message(message: Heartbeat, producer: &BaseProducer, topic: &str) -> Result<(), ()> {
        let json = serde_json::to_string(&message)
            .unwrap_or_else(|_| panic!("Failed to serialize heartbeat message {:?}", message));
        let record = BaseRecord::to(topic).payload(&json).key("renoir");
        producer
            .send(record)
            .unwrap_or_else(|_| panic!("Failed to send heartbeat message {:?}", message));
        Ok(())
    }

    pub fn start_receiver(&mut self, groups: GroupMap, id: u64) {
        log::debug!("Starting receiver");
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
            .set("auto.offset.reset", "latest")
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
                match consumer.poll(Timeout::After(Duration::from_secs(1))) {
                    Some(Ok(bmessage)) => {
                        let now = Instant::now();
                        let payload = bmessage.payload().unwrap();
                        let Heartbeat(group_name, parallelism) =
                            serde_json::from_slice(payload).unwrap();
                        log::debug!("Received heartbeat message ({group_name}, {parallelism})");

                        let mut times_guard = kafka_times.lock();
                        let mut groups_guard = kafka_groups.lock();

                        if groups_guard.contains_key(&group_name) {
                            let earlier: Instant = times_guard[&group_name];
                            if now.duration_since(earlier) > interval + Duration::from_secs(5) {
                                log::warn!("GROUP DISCONNECTED: {group_name}");
                                groups_guard.remove(&group_name);
                                times_guard.remove(&group_name);
                            } else {
                                times_guard.insert(group_name, now);
                            }
                        } else {
                            groups_guard.insert(group_name.clone(), parallelism);
                            times_guard.insert(group_name, now);
                        }
                    }
                    Some(Err(e)) => {
                        log::error!("Kafka failed: {e:?}");
                    }
                    None => {
                        log::warn!("Received empty message from Kafka")
                    }
                }
            }
        }));
    }
}

#[derive(Debug, Clone)]
pub struct WatermarkManager {
    /// Maps each group to a map that tracks for each coord the last time
    time_tracking: HashMap<GroupName, HashMap<CoordUInt, Timestamp>>,
    /// The current minimum time.
    ///
    /// It's tracked the minimum because the timestamp are monotonically increasing
    min_time: Timestamp,
    /// The mapping of current host connected
    groups: GroupMap,
}

impl WatermarkManager {
    pub fn new(groups: GroupMap) -> Self {
        WatermarkManager {
            time_tracking: HashMap::new(),
            min_time: -1,
            groups,
        }
    }

    /// Return a watermark if the tracked time for a group advance, otherwise returns a None
    pub fn update(
        &mut self,
        group: GroupName,
        timestamp: Timestamp,
        id: CoordUInt,
    ) -> Option<Timestamp> {
        let groups_guard = self.groups.lock();

        if !groups_guard.contains_key(&group) {
            log::warn!("Watermark received from a non connected source");
            return None;
        }

        let keys_to_remove: Vec<String> = self
            .time_tracking
            .keys()
            .filter(|k| !groups_guard.contains_key(*k))
            .cloned()
            .collect();
        for key in keys_to_remove {
            self.time_tracking.remove(&key);
        }

        log::debug!("Received Watermark {timestamp} from {group}:{id}");

        let inner_map = self.time_tracking.entry(group.clone()).or_default();
        if let Some(replica_time) = inner_map.get(&id) {
            assert!(*replica_time < timestamp);
        }
        inner_map.insert(id, timestamp);

        let expected_n: u64 = groups_guard.values().copied().sum();
        let received_n: u64 = self
            .time_tracking
            .values()
            .map(|im| im.len())
            .sum::<usize>() as u64;
        let min_time = self
            .time_tracking
            .iter()
            .flat_map(|(_, im)| im.values())
            .min()
            .copied()
            .unwrap_or(-1);
        if expected_n == received_n && min_time > self.min_time {
            log::debug!("Time advanced {min_time}");
            self.min_time = min_time;
            return Some(min_time);
        }

        None
    }
}
