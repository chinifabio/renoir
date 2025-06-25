use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use flume::{Receiver, Sender};
use futures::StreamExt;
use parking_lot::Mutex;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{CommitMode, Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig, Message,
};

use crate::{
    channel::{RecvError, RecvTimeoutError, TryRecvError},
    config::KafkaConfig,
    network::{NetworkReceiver, NetworkSendError, NetworkSender, ReceiverEndpoint},
    operator::ExchangeData,
};

impl<T: ExchangeData> From<(&KafkaConfig, ReceiverEndpoint)> for KafkaSenderInner<T> {
    fn from(value: (&KafkaConfig, ReceiverEndpoint)) -> Self {
        let (kafka_config, endpoint) = value;
        let mut producer_config = ClientConfig::new();
        let brokers = kafka_config.brokers.join(",");
        producer_config
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000");

        let producer = producer_config
            .create::<FutureProducer>()
            .expect("failed to create kafka producer");
        let topic = kafka_config.topic.clone();

        let (tx, rx): (Sender<T>, Receiver<T>) = flume::bounded(8);
        let cancel_token = Arc::new(AtomicBool::new(false));
        let cancel = cancel_token.clone();

        tokio::spawn(async move {
            loop {
                if cancel.load(Ordering::SeqCst) {
                    break;
                }
                match rx.recv() {
                    Ok(msg) => {
                        let payload =
                            bincode::serde::encode_to_vec(&msg, bincode::config::standard())
                                .expect("Failed to serialize item to send using kafka");
                        let message: FutureRecord<'_, [u8], [u8]> =
                            FutureRecord::to(topic.as_str()).payload(payload.as_slice());
                        match producer.send(message, Timeout::Never).await {
                            Ok(_) => {}
                            Err(e) => {
                                log::error!("Failed to send message using kafka: {e:?}");
                                todo!("failed to send message using kafka, implement a fail logic")
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to send message using kafka: {e:?}");
                        todo!()
                    }
                }
            }
        });

        Self {
            tx,
            cancel_token,
            endpoint,
        }
    }
}

impl<T: ExchangeData> From<&KafkaConfig> for KafkaReceiverInner<T> {
    fn from(kafka_config: &KafkaConfig) -> Self {
        let mut config = ClientConfig::new();
        let brokers = kafka_config.brokers.join(",");
        config
            .set("group.id", "asd")
            .set("bootstrap.servers", brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set_log_level(RDKafkaLogLevel::Info);

        let consumer = config
            .create::<StreamConsumer>()
            .expect("failed to create kafka consumer");
        let topics = vec![kafka_config.topic.clone()];
        let t = topics.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
        consumer
            .subscribe(t.as_slice())
            .expect("failed to subscribe to kafka topics");
        tracing::debug!("RenoirKafkaConsumer subscribed to {topics:?}");

        let (tx, rx) = flume::bounded(8);
        let cancel_token = Arc::new(AtomicBool::new(false));
        let cancel = cancel_token.clone();

        tokio::spawn(async move {
            let mut stream = consumer.stream();
            while let Some(msg) = stream.next().await {
                let msg = msg.expect("failed receiving from kafka");
                if cancel.load(Ordering::SeqCst) {
                    break;
                }
                let owned = msg.detach();
                let payload = owned
                    .payload()
                    .expect("Failed to retrive payload from kafka message");
                let (data, _) =
                    bincode::serde::decode_from_slice(payload, bincode::config::standard())
                        .expect("Failed to deserialize message payload from kafka");
                if let Err(e) = tx.send(data) {
                    if cancel.load(Ordering::SeqCst) {
                        break;
                    } else {
                        panic!("channel send failed for kafka consumer {e}");
                    }
                }
                consumer
                    .commit_message(&msg, CommitMode::Async)
                    .expect("kafka failed to commit");
            }
            tracing::debug!("RenoirKafkaConsumer background task terminated.");
        });

        Self { cancel_token, rx }
    }
}

#[allow(dead_code)]
pub(crate) fn kafka_channel<Out: ExchangeData>(
    config: &KafkaConfig,
    receiver_endpoint: ReceiverEndpoint,
) -> (NetworkSender<Out>, NetworkReceiver<Out>) {
    let sender = Arc::new(Mutex::new((config, receiver_endpoint).into()));
    let receiver = Arc::new(Mutex::new(config.into()));
    (
        NetworkSender {
            receiver_endpoint,
            sender: crate::network::SenderInner::Kafka(sender),
        },
        NetworkReceiver {
            receiver_endpoint,
            receiver: crate::network::ReceiverInner::Kafka(receiver),
        },
    )
}

/// Creates a Kafka sender channel.
pub(crate) fn kafka_sender<Out: ExchangeData>(
    config: &KafkaConfig,
    receiver_endpoint: ReceiverEndpoint,
) -> NetworkSender<Out> {
    let sender = Arc::new(Mutex::new((config, receiver_endpoint).into()));
    NetworkSender {
        receiver_endpoint,
        sender: crate::network::SenderInner::Kafka(sender),
    }
}

/// Creates a Kafka receiver channel.
pub(crate) fn kafka_receiver<Out: ExchangeData>(
    config: &KafkaConfig,
    receiver_endpoint: ReceiverEndpoint,
) -> NetworkReceiver<Out> {
    let receiver = Arc::new(Mutex::new(config.into()));
    NetworkReceiver {
        receiver_endpoint,
        receiver: crate::network::ReceiverInner::Kafka(receiver),
    }
}

pub(crate) type KafkaSender<T> = Arc<Mutex<KafkaSenderInner<T>>>;
pub(crate) type KafkaReceiver<T> = Arc<Mutex<KafkaReceiverInner<T>>>;

#[derive(Clone, Debug)]
pub(crate) struct KafkaSenderInner<T: Send + 'static> {
    tx: Sender<T>,
    cancel_token: Arc<AtomicBool>,
    endpoint: ReceiverEndpoint,
}

impl<T: Send + 'static> KafkaSenderInner<T> {
    /// Signals the background task to stop consuming messages.
    pub fn stop(&self) {
        self.cancel_token.store(true, Ordering::SeqCst);
    }

    /// Send a message in the channel, blocking if it's full.
    #[inline]
    pub fn send(&self, message: T) -> Result<(), crate::network::NetworkSendError> {
        self.tx.send(message).map_err(|e| {
            log::error!("Failed to send message using kafka: {e:?}");
            NetworkSendError::Disconnected(self.endpoint)
        })
    }

    #[inline]
    pub fn try_send(&self, message: T) -> Result<(), crate::network::NetworkSendError> {
        self.tx.try_send(message).map_err(|e| {
            log::error!("Failed to send message using kafka: {e:?}");
            NetworkSendError::Disconnected(self.endpoint)
        })
    }
}

impl<T: Send + 'static> Drop for KafkaSenderInner<T> {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Clone, Debug)]
pub(crate) struct KafkaReceiverInner<T: Send + 'static> {
    cancel_token: Arc<AtomicBool>,
    rx: Receiver<T>,
}

impl<T: Send + 'static> KafkaReceiverInner<T> {
    /// Signals the background task to stop consuming messages.
    pub fn stop(&self) {
        self.cancel_token.store(true, Ordering::SeqCst);
    }

    #[inline]
    pub fn recv(&self) -> Result<T, crate::channel::RecvError> {
        self.rx.recv().map_err(|e| {
            log::error!("Kafka recv error: {e:?}");
            RecvError::Disconnected
        })
    }

    #[inline]
    pub fn try_recv(&self) -> Result<T, crate::channel::TryRecvError> {
        self.rx.try_recv().map_err(|e| {
            log::error!("Kafka try_recv error: {e:?}");
            TryRecvError::Disconnected
        })
    }

    #[inline]
    pub fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<T, crate::channel::RecvTimeoutError> {
        self.rx.recv_timeout(timeout).map_err(|e| {
            log::error!("Kafka recv error: {e:?}");
            RecvTimeoutError::Disconnected
        })
    }

    #[inline]
    pub fn select<In2: crate::operator::ExchangeData>(
        &self,
        _other: &KafkaReceiverInner<In2>,
    ) -> crate::channel::SelectResult<T, In2> {
        todo!()
    }

    #[inline]
    pub fn select_timeout<In2: crate::operator::ExchangeData>(
        &self,
        _other: &KafkaReceiverInner<In2>,
        _timeout: std::time::Duration,
    ) -> Result<crate::channel::SelectResult<T, In2>, crate::channel::RecvTimeoutError> {
        todo!()
    }
}

impl<T: Send + 'static> Drop for KafkaReceiverInner<T> {
    fn drop(&mut self) {
        self.stop();
    }
}
