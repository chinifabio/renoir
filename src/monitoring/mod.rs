use std::thread::JoinHandle;

#[cfg(not(feature = "monitoring"))]
pub use local::*;
#[cfg(feature = "monitoring")]
pub use remote::*;

use crate::{config::RemoteConfig, runner::HostExecutionResult};

#[cfg(feature = "monitoring")]
pub mod actors;

pub(crate) trait Supervisor {
    fn new(
        status_rx: flume::Receiver<crate::worker::WorkerStatus>,
        worker_count: usize,
        terminate_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self
    where
        Self: Sized;

    fn run(self, config: &crate::config::RuntimeConfig) -> crate::worker::WorkerResult;
}

pub fn get_coordinator_handle(config: &RemoteConfig) -> Option<JoinHandle<HostExecutionResult>> {
    #[cfg(feature = "monitoring")]
    return config.monitoring.as_ref().map(|monitoring| {
        crate::monitoring::actors::start_coordinator_service(monitoring, config.hosts.len())
    });
    #[cfg(not(feature = "monitoring"))]
    {
        log::debug!("Monitoring is not enabled because the `monitoring` feature is not enabled");
        return None;
    }
}

#[cfg(not(feature = "monitoring"))]
mod local {
    use std::sync::{atomic::AtomicBool, Arc};

    use crate::{
        worker::{WorkerResult, WorkerStatus},
        RuntimeConfig,
    };

    /// Local supervisor that monitors workers on this host
    pub struct LocalSupervisor {
        /// Receiver for worker status updates
        pub status_rx: flume::Receiver<WorkerStatus>,
        /// Number of active workers
        pub active_workers: usize,
        /// Flag to signal termination of execution
        pub terminate_flag: Arc<AtomicBool>,
    }

    impl super::Supervisor for LocalSupervisor {
        fn new(
            status_rx: flume::Receiver<WorkerStatus>,
            worker_count: usize,
            terminate_flag: Arc<AtomicBool>,
        ) -> Self {
            Self {
                status_rx,
                active_workers: worker_count,
                terminate_flag,
            }
        }

        fn run(mut self, _config: &RuntimeConfig) -> WorkerResult {
            let mut fails = Vec::new();

            while self.active_workers > 0 {
                match self.status_rx.recv() {
                    Ok(WorkerStatus::Completed(coord)) => {
                        debug!(
                            "Worker {} completed. {} workers remaining",
                            coord, self.active_workers
                        );
                        self.active_workers -= 1;
                    }
                    Ok(WorkerStatus::Failed(coord, error)) => {
                        error!("Worker {} failed: {}", coord, error);
                        self.active_workers -= 1;
                        fails.push((coord, error.clone()));
                    }
                    Err(flume::RecvError::Disconnected) => {
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

            info!("all workers completed successfully");
            Ok(())
        }
    }
}

#[cfg(feature = "monitoring")]
pub mod remote {
    use crate::monitoring::Supervisor;

    pub use crate::monitoring::actors::LocalSupervisor;

    impl Supervisor for LocalSupervisor {
        fn new(
            status_rx: flume::Receiver<crate::worker::WorkerStatus>,
            worker_count: usize,
            terminate_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
        ) -> Self {
            Self {
                status_rx,
                active_workers: worker_count,
                terminate_flag,
                rt: tokio::runtime::Handle::current(),
            }
        }

        fn run(self, config: &crate::config::RuntimeConfig) -> crate::worker::WorkerResult {
            match config {
                crate::RuntimeConfig::Local(_) => {
                    panic!("Local runtime config should not be used with remote supervisor")
                }
                crate::RuntimeConfig::Remote(remote_config) => self.start(remote_config),
            }
        }
    }
}
