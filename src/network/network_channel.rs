use std::time::Duration;

use thiserror::Error;

use crate::channel::{
    self, Receiver, RecvError, RecvTimeoutError, SelectResult, Sender, TryRecvError, TrySendError,
};

use crate::network::{NetworkMessage, ReceiverEndpoint};
use crate::operator::ExchangeData;
use crate::profiler::{get_profiler, Profiler};

/// The capacity of the in-buffer.
const CHANNEL_CAPACITY: usize = 16;

pub(crate) fn local_channel<T: ExchangeData>(
    receiver_endpoint: ReceiverEndpoint,
) -> (NetworkSender<T>, NetworkReceiver<T>) {
    let (sender, receiver) = channel::bounded(CHANNEL_CAPACITY);
    (
        NetworkSender {
            receiver_endpoint,
            sender: SenderInner::Local(sender),
        },
        NetworkReceiver {
            receiver_endpoint,
            receiver: ReceiverInner::Legacy(receiver),
        },
    )
}

pub(crate) fn mux_sender<T: ExchangeData>(
    receiver_endpoint: ReceiverEndpoint,
    tx: Sender<(ReceiverEndpoint, NetworkMessage<T>)>,
) -> NetworkSender<T> {
    NetworkSender {
        receiver_endpoint,
        sender: SenderInner::Mux(tx),
    }
}

/// The receiving end of a connection between two replicas.
///
/// This works for both a local in-memory connection and for a remote socket connection. This will
/// always be able to listen to a socket. No socket will be bound until a message is sent to the
/// starter returned by the constructor.
///
/// Internally it contains a in-memory sender-receiver pair, to get the local sender call
/// `.sender()`. When the socket will be bound an task will be spawned, it will bind the
/// socket and send to the same in-memory channel the received messages.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct NetworkReceiver<In: Send + 'static, R: NetworkReceiverInner<In> = ()> {
    /// The ReceiverEndpoint of the current receiver.
    pub receiver_endpoint: ReceiverEndpoint,
    /// The actual receiver where the users of this struct will wait upon.
    #[derivative(Debug = "ignore")]
    receiver: ReceiverInner<In, R>,
}

enum ReceiverInner<In: Send + 'static, R: NetworkReceiverInner<In>> {
    Legacy(Receiver<NetworkMessage<In>>),
    Custom(R),
}

pub(crate) trait NetworkReceiverInner<In: Send + 'static> {
    fn recv(&self) -> Result<NetworkMessage<In>, RecvError>;

    /// Receive a message from any sender without blocking.
    fn try_recv(&self) -> Result<NetworkMessage<In>, TryRecvError>;

    /// Receive a message from any sender with a timeout.
    fn recv_timeout(&self, timeout: Duration) -> Result<NetworkMessage<In>, RecvTimeoutError>;

    /// Receive a message from any sender of this receiver of the other provided receiver.
    ///
    /// The first message of the two is returned. If both receivers are ready one of them is chosen
    /// randomly (with an unspecified probability). It's guaranteed this function has the eventual
    /// fairness property.
    fn select<In2: ExchangeData>(
        &self,
        other: &NetworkReceiver<In2>,
    ) -> SelectResult<NetworkMessage<In>, NetworkMessage<In2>>;

    /// Same as `select`, with a timeout.
    fn select_timeout<In2: ExchangeData>(
        &self,
        other: &NetworkReceiver<In2>,
        timeout: Duration,
    ) -> Result<SelectResult<NetworkMessage<In>, NetworkMessage<In2>>, RecvTimeoutError>;
}

impl<In: Send + 'static> NetworkReceiverInner<In> for () {
    fn recv(&self) -> Result<NetworkMessage<In>, RecvError> {
        unreachable!()
    }

    fn try_recv(&self) -> Result<NetworkMessage<In>, TryRecvError> {
        unreachable!()
    }

    fn recv_timeout(&self, _: Duration) -> Result<NetworkMessage<In>, RecvTimeoutError> {
        unreachable!()
    }

    fn select<In2: ExchangeData>(
        &self,
        _: &NetworkReceiver<In2>,
    ) -> SelectResult<NetworkMessage<In>, NetworkMessage<In2>> {
        unreachable!()
    }

    fn select_timeout<In2: ExchangeData>(
        &self,
        _: &NetworkReceiver<In2>,
        _: Duration,
    ) -> Result<SelectResult<NetworkMessage<In>, NetworkMessage<In2>>, RecvTimeoutError> {
        unreachable!()
    }
}

impl<In: Send + 'static> NetworkReceiver<In> {
    #[inline]
    fn profile_message<E>(
        &self,
        message: Result<NetworkMessage<In>, E>,
    ) -> Result<NetworkMessage<In>, E> {
        message.inspect(|message| {
            get_profiler().items_in(
                message.sender,
                self.receiver_endpoint.coord,
                message.num_items(),
            );
        })
    }

    /// Receive a message from any sender.
    pub fn recv(&self) -> Result<NetworkMessage<In>, RecvError> {
        let data = match &self.receiver {
            ReceiverInner::Legacy(rx) => rx.recv(),
            ReceiverInner::Custom(rx) => rx.recv(),
        };
        self.profile_message(data)
    }

    /// Receive a message from any sender without blocking.
    pub fn try_recv(&self) -> Result<NetworkMessage<In>, TryRecvError> {
        let data = match &self.receiver {
            ReceiverInner::Legacy(rx) => rx.try_recv(),
            ReceiverInner::Custom(rx) => rx.try_recv(),
        };
        self.profile_message(data)
    }

    /// Receive a message from any sender with a timeout.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<NetworkMessage<In>, RecvTimeoutError> {
        let data = match &self.receiver {
            ReceiverInner::Legacy(rx) => rx.recv_timeout(timeout),
            ReceiverInner::Custom(rx) => rx.recv_timeout(timeout),
        };
        self.profile_message(data)
    }

    /// Receive a message from any sender of this receiver of the other provided receiver.
    ///
    /// The first message of the two is returned. If both receivers are ready one of them is chosen
    /// randomly (with an unspecified probability). It's guaranteed this function has the eventual
    /// fairness property.
    pub fn select<In2: ExchangeData, R2: NetworkReceiverInner<In2>>(
        &self,
        other: &NetworkReceiver<In2, R2>,
    ) -> SelectResult<NetworkMessage<In>, NetworkMessage<In2>> {
        match (&self.receiver, &other.receiver) {
            (ReceiverInner::Legacy(rx), ReceiverInner::Legacy(other)) => rx.select(other),
            _ => todo!(),
        }
    }

    /// Same as `select`, with a timeout.
    pub fn select_timeout<In2: ExchangeData>(
        &self,
        other: &NetworkReceiver<In2>,
        timeout: Duration,
    ) -> Result<SelectResult<NetworkMessage<In>, NetworkMessage<In2>>, RecvTimeoutError> {
        match (&self.receiver, &other.receiver) {
            (ReceiverInner::Legacy(rx), ReceiverInner::Legacy(other)) => {
                rx.select_timeout(other, timeout)
            }
            _ => todo!(),
        }
    }
}

/// The sender part of a connection between two replicas.
///
/// This works for both a local in-memory connection and for a remote socket connection. When this
/// is bound to a local channel the receiver will be a `Receiver`. When it's bound to a remote
/// connection internally this points to the multiplexer that handles the remote channel.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(crate) struct NetworkSender<Out: Send + 'static, S: NetworkSenderInner<Out> = ()> {
    /// The ReceiverEndpoint of the recipient.
    pub receiver_endpoint: ReceiverEndpoint,
    /// The generic sender that will send the message either locally or remotely.
    #[derivative(Debug = "ignore")]
    sender: SenderInner<Out, S>,
}

pub(crate) trait NetworkSenderInner<Out>: Clone {
    fn send(&self, message: NetworkMessage<Out>) -> Result<(), NetworkSendError>;
    fn try_send(&self, message: NetworkMessage<Out>) -> Result<(), NetworkSendError>;
}

impl<Out> NetworkSenderInner<Out> for () {
    fn send(&self, _: NetworkMessage<Out>) -> Result<(), NetworkSendError> {
        unreachable!()
    }

    fn try_send(&self, _: NetworkMessage<Out>) -> Result<(), NetworkSendError> {
        unreachable!()
    }
}

enum SenderInner<Out: Send + 'static, S: NetworkSenderInner<Out>> {
    Mux(Sender<(ReceiverEndpoint, NetworkMessage<Out>)>),
    Local(Sender<NetworkMessage<Out>>),
    Custom(S),
}

impl<Out: Send + 'static, S: NetworkSenderInner<Out>> Clone for SenderInner<Out, S> {
    fn clone(&self) -> Self {
        match self {
            Self::Mux(arg0) => Self::Mux(arg0.clone()),
            Self::Local(arg0) => Self::Local(arg0.clone()),
            Self::Custom(arg0) => Self::Custom(arg0.clone()),
        }
    }
}

impl<Out: Send + 'static, S: NetworkSenderInner<Out>> NetworkSender<Out, S> {
    pub fn send(&self, message: NetworkMessage<Out>) -> Result<(), NetworkSendError> {
        get_profiler().items_out(
            message.sender,
            self.receiver_endpoint.coord,
            message.num_items(),
        );

        match &self.sender {
            SenderInner::Mux(tx) => tx
                .send((self.receiver_endpoint, message))
                .map_err(|_| NetworkSendError::Disconnected(self.receiver_endpoint)),
            SenderInner::Local(tx) => tx
                .send(message)
                .map_err(|_| NetworkSendError::Disconnected(self.receiver_endpoint)),
            SenderInner::Custom(tx) => tx
                .send(message)
                .map_err(|_| NetworkSendError::Disconnected(self.receiver_endpoint)),
        }
    }

    pub fn try_send(&self, message: NetworkMessage<Out>) -> Result<(), NetworkTrySendError<Out>> {
        let sender = message.sender;
        let size = message.num_items();
        let res = match &self.sender {
            SenderInner::Mux(tx) => {
                tx.try_send((self.receiver_endpoint, message))
                    .map_err(|e| match e {
                        TrySendError::Full(item) => NetworkTrySendError::Full(item.1),
                        TrySendError::Disconnected(_) => {
                            NetworkTrySendError::Disconnected(self.receiver_endpoint)
                        }
                    })
            }
            SenderInner::Local(tx) => tx.try_send(message).map_err(|e| match e {
                TrySendError::Full(item) => NetworkTrySendError::Full(item),
                TrySendError::Disconnected(_) => {
                    NetworkTrySendError::Disconnected(self.receiver_endpoint)
                }
            }),
            SenderInner::Custom(tx) => tx.try_send(message).map_err(|e| match e {
                NetworkSendError::Disconnected(_) => {
                    NetworkTrySendError::Disconnected(self.receiver_endpoint)
                }
            }),
        };
        if res.is_ok() {
            get_profiler().items_out(sender, self.receiver_endpoint.coord, size);
        }
        res
    }

    pub fn clone_inner(&self) -> Sender<NetworkMessage<Out>> {
        match &self.sender {
            SenderInner::Mux(_) => panic!("Trying to clone mux channel. Not supported"),
            SenderInner::Local(tx) => tx.clone(),
            SenderInner::Custom(_) => todo!(),
        }
    }
}

#[derive(Debug, Error)]
pub enum NetworkSendError {
    #[error("channel disconnected")]
    Disconnected(ReceiverEndpoint),
}

#[derive(Debug, Error)]
pub enum NetworkTrySendError<T> {
    #[error("channel full")]
    Full(NetworkMessage<T>),
    #[error("channel disconnected")]
    Disconnected(ReceiverEndpoint),
}
