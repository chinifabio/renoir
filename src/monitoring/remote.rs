use std::thread::JoinHandle;

use futures::{SinkExt, StreamExt};
use tokio::task::JoinSet;
use tokio_util::{bytes::Bytes, codec::{Framed, LengthDelimitedCodec}};

use crate::{
    config::MonitoringConfig, monitoring::local::LocalSupervisor, network::Coord, runner::HostExecutionResult, worker::{WorkerResult, WorkerStatus}
};

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) enum WorkerExecutionMessage {
    StatusUpdate(WorkerStatus),
    Heartbeat,
}

impl WorkerExecutionMessage {
    pub fn completed(coord: Coord) -> Self {
        Self::StatusUpdate(WorkerStatus::Completed(coord))
    }

    pub fn failed(coord: Coord, error: String) -> Self {
        Self::StatusUpdate(WorkerStatus::Failed(coord, error))
    }

    pub fn heartbeat() -> Self {
        Self::Heartbeat
    }
}

impl LocalSupervisor {
    /// Monitor workers until all complete or one fails
    /// Returns Ok if all workers complete successfully, Err with coord and message on failure
    pub async fn run_remote(mut self, config: &MonitoringConfig) -> WorkerResult {
        info!(
            "running in remote mode with {} workers (check interval: {:?})",
            self.active_workers, config.collection_interval
        );

        let stream =
            tokio::net::TcpStream::connect((config.coordinator_address.as_ref(), config.port))
                .await
                .map_err(|e| {
                    format!(
                        "Failed to connect to coordinator at {:?}:{:?}: {}",
                        config.coordinator_address, config.port, e
                    )
                })?;
        let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

        let mut fails = Vec::new();

        let check_interval = std::time::Duration::from_secs(config.collection_interval);
        while self.active_workers > 0 {
            match self.status_rx.recv_timeout(check_interval) {
                Ok(WorkerStatus::Completed(coord)) => {
                    debug!(
                        "Worker {} completed. {} workers remaining",
                        coord, self.active_workers
                    );
                    self.active_workers -= 1;

                    let data = WorkerExecutionMessage::completed(coord);
                    let raw_data =
                        bincode::serde::encode_to_vec(&data, BINCODE_CONFIG).map_err(|e| {
                            format!(
                                "Failed to serialize worker status update for {}: {}",
                                coord, e
                            )
                        })?;
                    framed.send(Bytes::from(raw_data)).await.map_err(|e| {
                        format!("Failed to send worker status update for {}: {}", coord, e)
                    })?;
                }
                Ok(WorkerStatus::Failed(coord, error)) => {
                    error!("Worker {} failed: {}", coord, error);
                    self.active_workers -= 1;
                    fails.push((coord, error.clone()));

                    let data = WorkerExecutionMessage::failed(coord, error);
                    let raw_data =
                        bincode::serde::encode_to_vec(&data, BINCODE_CONFIG).map_err(|e| {
                            format!(
                                "Failed to serialize worker status update for {}: {}",
                                coord, e
                            )
                        })?;
                    framed.send(Bytes::from(raw_data)).await.map_err(|e| {
                        format!("Failed to send worker status update for {}: {}", coord, e)
                    })?;
                }
                Err(flume::RecvTimeoutError::Timeout) => {
                    debug!("{} workers still active", self.active_workers);

                    let data = WorkerExecutionMessage::heartbeat();
                    let raw_data =
                        bincode::serde::encode_to_vec(&data, BINCODE_CONFIG).map_err(|e| {
                            format!(
                                "Failed to serialize worker status update for heartbeat: {}",
                                e
                            )
                        })?;
                    framed.send(Bytes::from(raw_data)).await.map_err(|e| {
                        format!("Failed to send worker status update for heartbeat: {}", e)
                    })?;
                }
                Err(flume::RecvTimeoutError::Disconnected) => {
                    error!(
                        "Status channel disconnected with {} workers still active",
                        self.active_workers
                    );
                    return Err(format!(
                        "Status channel disconnected with {} workers remaining",
                        self.active_workers
                    ));
                }
            }
        }

        if !fails.is_empty() {
            return Err(format!("{} workers failed: {:?}", fails.len(), fails));
        }

        Ok(())
    }
}

#[derive(Debug)]
struct CoordinatorService {
    config: MonitoringConfig,
}

impl From<&MonitoringConfig> for CoordinatorService {
    fn from(config: &MonitoringConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }
}

impl CoordinatorService {
    pub async fn run(&self, mut expected_worker: usize) -> Result<(), String> {
        let addr = (self.config.bind_address.as_ref(), self.config.port);
        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            format!("Failed to bind to {:?}: {}", addr, e)
        })?;

        log::info!(
            "coordinator service started on {:?}:{:?}",
            self.config.bind_address,
            self.config.port
        );

        let mut connections = JoinSet::new();
        while expected_worker > 0 {
            let (socket, addr) = listener.accept().await.map_err(|e| {
                format!("Failed to accept connection: {}", e)
            })?;
            log::debug!("Accepted connection from {:?}", addr);
            connections.spawn(handle_connection(socket));
            expected_worker -= 1;
        }

        while let Some(res) = connections.join_next().await {
            if let Err(e) = res {
                log::error!("Error in connection handler: {}", e);
            }
        }

        Ok(())
    }
}

#[cfg(feature = "tokio")]
pub(crate) fn start_coordinator_service(
    monitoring: &crate::config::MonitoringConfig,
    expected_worker: usize,
) -> JoinHandle<HostExecutionResult> {
    let manager = CoordinatorService::from(monitoring);
    std::thread::Builder::new()
        .name("coordinator".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .unwrap();
            rt.block_on(async {
                manager
                    .run(expected_worker)
                    .await
                    .expect("Failed to run manager");
            });
            Default::default()
        })
        .unwrap()
}

async fn handle_connection(socket: tokio::net::TcpStream) {
    let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
    while let Some(frame) = framed.next().await {
        match frame {
            Ok(bytes) => {
                match bincode::serde::decode_from_slice::<WorkerExecutionMessage, _>(
                    &bytes,
                    BINCODE_CONFIG,
                ) {
                    Ok((message, _)) => {
                        log::debug!("Received message: {:?}", message);
                        // Handle the message as needed (e.g., update internal state, log results, etc.)
                    }
                    Err(e) => {
                        log::error!("Failed to decode message: {}", e);
                    }
                }
            }
            Err(e) => {
                log::error!("Error handling connection: {}", e);
                break;
            }
        }
    }
}
