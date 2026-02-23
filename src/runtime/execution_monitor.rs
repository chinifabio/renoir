use std::sync::Arc;

use futures::SinkExt;
use tokio_util::{bytes::Bytes, codec::{Framed, LengthDelimitedCodec}};

use crate::{RuntimeConfig, config::MonitoringConfig, runtime::{BINCODE_CONFIG, WorkerExecutionMessage}, worker::{WorkerResult, WorkerStatus}};

/// Local supervisor that monitors workers on this host
pub struct ExecutionMonitor {
    /// Receiver for worker status updates
    status_rx: flume::Receiver<WorkerStatus>,
    /// Number of active workers
    active_workers: usize,
    /// Configuration for monitoring intervals
    config: Option<MonitoringConfig>,
}

impl ExecutionMonitor {
    pub fn new(
        status_rx: flume::Receiver<WorkerStatus>,
        worker_count: usize,
        config: Arc<RuntimeConfig>,
    ) -> Self {
        let config = match &*config {
            RuntimeConfig::Local(_) => None,
            RuntimeConfig::Remote(remote_config) => remote_config.monitoring.clone(),
        };
        Self {
            status_rx,
            active_workers: worker_count,
            config,
        }
    }

    /// Monitor workers until all complete or one fails
    /// Returns Ok if all workers complete successfully, Err with coord and message on failure
    pub async fn monitor(mut self) -> WorkerResult {
        let config = self.config.take().unwrap_or_default();
        info!(
            "ExecutionMonitor monitoring {} workers (check interval: {:?})",
            self.active_workers, config.collection_interval
        );

        let stream = tokio::net::TcpStream::connect((config.coordinator_address.as_ref(), config.port)).await.map_err(|e| {
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
                    let raw_data = bincode::serde::encode_to_vec(&data, BINCODE_CONFIG).map_err(|e| {
                        format!("Failed to serialize worker status update for {}: {}", coord, e)
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
                    let raw_data = bincode::serde::encode_to_vec(&data, BINCODE_CONFIG).map_err(|e| {
                        format!("Failed to serialize worker status update for {}: {}", coord, e)
                    })?;
                    framed.send(Bytes::from(raw_data)).await.map_err(|e| {
                        format!("Failed to send worker status update for {}: {}", coord, e)
                    })?;
                }
                Err(flume::RecvTimeoutError::Timeout) => {
                    debug!(
                        "ExecutionMonitor heartbeat: {} workers still active",
                        self.active_workers
                    );

                    let data = WorkerExecutionMessage::heartbeat();
                    let raw_data = bincode::serde::encode_to_vec(&data, BINCODE_CONFIG).map_err(|e| {
                        format!("Failed to serialize worker status update for heartbeat: {}", e)
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

        info!("ExecutionMonitor: all workers completed successfully");
        Ok(())
    }

    pub fn monitor_sync(self) -> WorkerResult {
        Ok(()) // Placeholder for synchronous monitoring logic if needed in the future
    }
}
