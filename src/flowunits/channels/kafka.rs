use crate::{
    config::KafkaConfig,
    network::{NetworkReceiverInner, NetworkSenderInner},
};

#[derive(Clone)]
pub(crate) struct KafkaSender {
    config: KafkaConfig,
}

impl<T> NetworkSenderInner<T> for KafkaSender {
    fn send(
        &self,
        message: crate::network::NetworkMessage<T>,
    ) -> Result<(), crate::network::NetworkSendError> {
        todo!()
    }

    fn try_send(
        &self,
        message: crate::network::NetworkMessage<T>,
    ) -> Result<(), crate::network::NetworkSendError> {
        todo!()
    }
}

pub(crate) struct KafkaReceiver {
    config: KafkaConfig,
}

impl<T: std::marker::Send + 'static> NetworkReceiverInner<T> for KafkaReceiver {
    fn recv(&self) -> Result<crate::network::NetworkMessage<T>, crate::channel::RecvError> {
        todo!()
    }

    fn try_recv(&self) -> Result<crate::network::NetworkMessage<T>, crate::channel::TryRecvError> {
        todo!()
    }

    fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<crate::network::NetworkMessage<T>, crate::channel::RecvTimeoutError> {
        todo!()
    }

    fn select<In2: crate::operator::ExchangeData>(
        &self,
        other: &crate::network::NetworkReceiver<In2>,
    ) -> crate::channel::SelectResult<
        crate::network::NetworkMessage<T>,
        crate::network::NetworkMessage<In2>,
    > {
        todo!()
    }

    fn select_timeout<In2: crate::operator::ExchangeData>(
        &self,
        other: &crate::network::NetworkReceiver<In2>,
        timeout: std::time::Duration,
    ) -> Result<
        crate::channel::SelectResult<
            crate::network::NetworkMessage<T>,
            crate::network::NetworkMessage<In2>,
        >,
        crate::channel::RecvTimeoutError,
    > {
        todo!()
    }
}
