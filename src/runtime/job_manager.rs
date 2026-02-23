use std::thread::JoinHandle;

use futures::StreamExt;
use tokio::task::JoinSet;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::{config::MonitoringConfig, runner::HostExecutionResult, runtime::{BINCODE_CONFIG, WorkerExecutionMessage}};

#[derive(Debug)]
struct JobManager {
    config: MonitoringConfig,
}

impl From<&MonitoringConfig> for JobManager {
    fn from(config: &MonitoringConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }
}

impl JobManager {
    pub async fn run(&self, mut expected_worker: usize) -> Result<(), JobManagerError> {
        let addr = (self.config.bind_address.as_ref(), self.config.port);
        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            JobManagerError::StartError(format!("Failed to bind to {:?}: {}", addr, e))
        })?;

        log::info!("Monitoring server started on {:?}:{:?}", self.config.bind_address, self.config.port);

        let mut connections = JoinSet::new();
        while expected_worker > 0 {
            let (socket, addr) = listener.accept().await.map_err(|e| {
                JobManagerError::StartError(format!("Failed to accept connection: {}", e))
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
pub(crate) fn start_monitoring_server(
    monitoring: &crate::config::MonitoringConfig,
    expected_worker: usize,
) -> JoinHandle<HostExecutionResult> {
    let manager = JobManager::from(monitoring);
    std::thread::Builder::new()
        .name("monitoring".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .unwrap();
            rt.block_on(async {
                manager.run(expected_worker).await.expect("Failed to run manager");
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
                match bincode::serde::decode_from_slice::<WorkerExecutionMessage, _>(&bytes, BINCODE_CONFIG) {
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

#[derive(Debug)]
pub enum JobManagerError {
    /// An error occurred while starting the monitoring server.
    StartError(String),
}
