use std::{marker::PhantomData, sync::Arc};

use parking_lot::Mutex;

use crate::{
    config::KafkaConfig,
    network::{NetworkMessage, NetworkReceiver, NetworkSender, ReceiverEndpoint},
};

impl<T: Send + 'static> From<&KafkaConfig> for KafkaSenderInner<NetworkMessage<T>> {
    fn from(value: &KafkaConfig) -> Self {
        todo!()
    }
}

impl<T: Send + 'static> From<&KafkaConfig> for KafkaReceiverInner<NetworkMessage<T>> {
    fn from(value: &KafkaConfig) -> Self {
        todo!()
    }
}

pub(crate) fn kafka_channel<Out: Send + 'static>(
    config: &KafkaConfig,
    receiver_endpoint: ReceiverEndpoint,
) -> (NetworkSender<Out>, NetworkReceiver<Out>) {
    let sender = Arc::new(Mutex::new(config.into()));
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

pub(crate) type KafkaSender<T> = Arc<Mutex<KafkaSenderInner<T>>>;
pub(crate) type KafkaReceiver<T> = Arc<Mutex<KafkaReceiverInner<T>>>;

#[derive(Clone, Debug)]
pub(crate) struct KafkaSenderInner<T: Send + 'static> {
    _t: PhantomData<T>,
}

impl<T: Send + 'static> KafkaSenderInner<T> {
    /// Send a message in the channel, blocking if it's full.
    #[inline]
    pub fn send(&self, message: T) -> Result<(), crate::network::NetworkSendError> {
        todo!()
    }

    #[inline]
    pub fn try_send(&self, message: T) -> Result<(), crate::network::NetworkSendError> {
        todo!()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct KafkaReceiverInner<T: Send + 'static> {
    _t: PhantomData<T>,
}

impl<T: Send + 'static> KafkaReceiverInner<T> {
    #[inline]
    pub fn recv(&self) -> Result<T, crate::channel::RecvError> {
        todo!()
    }

    #[inline]
    pub fn try_recv(&self) -> Result<T, crate::channel::TryRecvError> {
        todo!()
    }

    #[inline]
    pub fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<T, crate::channel::RecvTimeoutError> {
        todo!()
    }

    #[inline]
    pub fn select<In2: crate::operator::ExchangeData>(
        &self,
        other: &In2,
    ) -> crate::channel::SelectResult<T, In2> {
        todo!()
    }

    #[inline]
    pub fn select_timeout<In2: crate::operator::ExchangeData>(
        &self,
        other: &In2,
        timeout: std::time::Duration,
    ) -> Result<crate::channel::SelectResult<T, In2>, crate::channel::RecvTimeoutError> {
        todo!()
    }
}
