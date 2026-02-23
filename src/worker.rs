use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WorkerStatus {
    /// Worker completed successfully
    Completed(Coord),
    /// Worker failed with an error
    Failed(Coord, String),
}

/// Result type for worker execution
pub type WorkerResult = Result<(), String>;

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
    terminate_flag: Arc<AtomicBool>,
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
            do_work(block, coord, status_tx, terminate_flag)
        })
        .unwrap();

    (join_handle, structure)
}

fn do_work<Op: Operator>(
    mut block: Block<Op>,
    coord: Coord,
    status_tx: flume::Sender<WorkerStatus>,
    terminate_flag: Arc<AtomicBool>,
) -> WorkerResult {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        while !matches!(block.operators.next(), StreamElement::Terminate) {
            if terminate_flag.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
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
