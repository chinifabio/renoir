use crate::worker::{WorkerResult, WorkerStatus};

/// Local supervisor that monitors workers on this host
pub struct ExecutionMonitor {
    /// Receiver for worker status updates
    status_rx: flume::Receiver<WorkerStatus>,
    /// Number of active workers
    active_workers: usize,
    /// Timeout for checking worker status
    check_interval: std::time::Duration,
}

impl ExecutionMonitor {
    pub fn new(
        status_rx: flume::Receiver<WorkerStatus>,
        worker_count: usize,
        check_interval: std::time::Duration,
    ) -> Self {
        Self {
            status_rx,
            active_workers: worker_count,
            check_interval,
        }
    }

    /// Monitor workers until all complete or one fails
    /// Returns Ok if all workers complete successfully, Err with coord and message on failure
    pub fn monitor(mut self) -> WorkerResult {
        info!(
            "ExecutionMonitor monitoring {} workers (check interval: {:?})",
            self.active_workers, self.check_interval
        );

        let mut fails = Vec::new();

        while self.active_workers > 0 {
            match self.status_rx.recv_timeout(self.check_interval) {
                Ok(WorkerStatus::Completed(coord)) => {
                    self.active_workers -= 1;
                    debug!(
                        "Worker {} completed. {} workers remaining",
                        coord, self.active_workers
                    );
                }
                Ok(WorkerStatus::Failed(coord, error)) => {
                    error!("Worker {} failed: {}", coord, error);
                    self.active_workers -= 1;
                    fails.push((coord, error));
                }
                Err(flume::RecvTimeoutError::Timeout) => {
                    debug!(
                        "ExecutionMonitor heartbeat: {} workers still active",
                        self.active_workers
                    );
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
}
