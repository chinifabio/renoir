use std::cell::RefCell;
use std::thread::JoinHandle;

use crate::block::{Block, BlockStructure};
use crate::network::Coord;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

thread_local! {
    /// Coordinates of the replica the current worker thread is working on.
    ///
    /// Access to this by calling `replica_coord()`.
    static COORD: RefCell<Option<Coord>> = const { RefCell::new(None) };
}

/// Status updates that workers send to the local supervisor
#[derive(Debug, Clone)]
pub enum WorkerStatus {
    /// Worker completed successfully
    Completed(Coord),
    /// Worker failed with an error
    Failed(Coord, String),
}

/// Result type for worker execution
pub type WorkerResult = Result<(), String>;

/// Local supervisor that monitors workers on this host
pub struct LocalSupervisor {
    /// Receiver for worker status updates
    status_rx: flume::Receiver<WorkerStatus>,
    /// Number of active workers
    active_workers: usize,
    /// Timeout for checking worker status
    check_interval: std::time::Duration,
}

impl LocalSupervisor {
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
            "LocalSupervisor monitoring {} workers (check interval: {:?})",
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
                        "LocalSupervisor heartbeat: {} workers still active",
                        self.active_workers
                    );
                }
                Err(flume::RecvTimeoutError::Disconnected) => {
                    error!(
                        "Status channel disconnected with {} workers still active",
                        self.active_workers
                    );
                    return Err(
                        format!(
                            "Status channel disconnected with {} workers remaining",
                            self.active_workers
                        ),
                    );
                }
            }
        }

        if !fails.is_empty() {
            return Err(format!("{} workers failed: {:?}", fails.len(), fails));
        }

        info!("LocalSupervisor: all workers completed successfully");
        Ok(())
    }

    /// Get a reference to demonstrate liveness
    pub fn is_alive(&self) -> bool {
        self.active_workers > 0
    }
}

/// Get the coord of the replica the current thread is working on.
///
/// This will return `Some(coord)` only when called from a worker thread of a replica, otherwise
/// `None` is returned.
pub fn replica_coord() -> Option<Coord> {
    COORD.with(|x| *x.borrow())
}

pub(crate) fn spawn_worker<OperatorChain>(
    mut block: Block<OperatorChain>,
    metadata: &mut ExecutionMetadata,
    status_tx: flume::Sender<WorkerStatus>,
) -> (JoinHandle<WorkerResult>, BlockStructure)
where
    OperatorChain: Operator + 'static,
    OperatorChain::Out: Send,
{
    let coord = metadata.coord;

    debug!("starting worker {}: {}", coord, block.to_string(),);

    block.operators.setup(metadata);
    let structure = block.operators.structure();

    let join_handle = std::thread::Builder::new()
        .name(format!("block-{}", block.id))
        .spawn(move || {
            // remember in the thread-local the coordinate of this block
            COORD.with(|x| *x.borrow_mut() = Some(coord));
            do_work(block, coord, status_tx)
        })
        .unwrap();

    (join_handle, structure)
}

fn do_work<Op: Operator>(
    mut block: Block<Op>,
    coord: Coord,
    status_tx: flume::Sender<WorkerStatus>,
) -> WorkerResult {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        while !matches!(block.operators.next(), StreamElement::Terminate) {
            // nothing to do
        }
    }));

    match result {
        Ok(_) => {
            info!("worker {} completed", coord);
            // Ignore send errors - supervisor might have already terminated
            let _ = status_tx.send(WorkerStatus::Completed(coord));
            Ok(())
        }
        Err(panic_info) => {
            let error_msg = if let Some(s) = panic_info.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                s.to_string()
            } else {
                "Unknown panic".to_string()
            };
            error!("worker {} crashed: {}", coord, error_msg);
            let _ = status_tx.send(WorkerStatus::Failed(coord, error_msg.clone()));
            Err(error_msg)
        }
    }
}
