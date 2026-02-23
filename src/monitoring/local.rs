use std::sync::{atomic::AtomicBool, Arc};


use crate::worker::{WorkerResult, WorkerStatus};

/// Local supervisor that monitors workers on this host
pub struct LocalSupervisor {
    /// Receiver for worker status updates
    pub status_rx: flume::Receiver<WorkerStatus>,
    /// Number of active workers
    pub active_workers: usize,
    /// Flag to signal termination of execution
    pub terminate_flag: Arc<AtomicBool>,
}

impl LocalSupervisor {
    pub fn new(
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

    pub fn run_local(mut self) -> WorkerResult {
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
                    error!("Status channel disconnected with {} workers still active", self.active_workers);
                    return Err(format!("Status channel disconnected with {} workers remaining", self.active_workers));
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
