use std::cell::RefCell;
#[cfg(feature = "actors")]
use std::sync::atomic::AtomicU8;
#[cfg(feature = "actors")]
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::block::{Block, BlockStructure};
#[cfg(feature = "actors")]
use crate::flowunits::actors::WorkerStatus;
use crate::network::Coord;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

thread_local! {
    /// Coordinates of the replica the current worker thread is working on.
    ///
    /// Access to this by calling `replica_coord()`.
    static COORD: RefCell<Option<Coord>> = const { RefCell::new(None) };
}

/// Get the coord of the replica the current thread is working on.
///
/// This will return `Some(coord)` only when called from a worker thread of a replica, otherwise
/// `None` is returned.
pub fn replica_coord() -> Option<Coord> {
    COORD.with(|x| *x.borrow())
}

/// Call a function if this struct goes out of scope without calling `defuse`, including during a
/// panic stack-unwinding.
struct CatchPanic<F: FnOnce()> {
    /// True if the function should be called.
    primed: bool,
    /// Function to call.
    ///
    /// The `Drop` implementation will move out the function.
    handler: Option<F>,
}

impl<F: FnOnce()> CatchPanic<F> {
    fn new(handler: F) -> Self {
        Self {
            primed: true,
            handler: Some(handler),
        }
    }

    /// Avoid calling the function on drop.
    fn defuse(&mut self) {
        self.primed = false;
    }
}

impl<F: FnOnce()> Drop for CatchPanic<F> {
    fn drop(&mut self) {
        if self.primed {
            (self.handler.take().unwrap())();
        }
    }
}

pub(crate) fn spawn_worker<OperatorChain>(
    mut block: Block<OperatorChain>,
    metadata: &mut ExecutionMetadata,
) -> (JoinHandle<()>, BlockStructure)
where
    OperatorChain: Operator + 'static,
    OperatorChain::Out: Send,
{
    let coord = metadata.coord;

    // Used to signal the status of the worker thread.
    // 0 = running
    // 1 = completed
    // 2 = crashed
    #[cfg(feature = "actors")]
    let status_flag = Arc::new(AtomicU8::new(WorkerStatus::Running as u8));

    debug!("starting worker {}: {}", coord, block.to_string(),);

    block.operators.setup(metadata);
    let structure = block.operators.structure();

    let join_handle = std::thread::Builder::new()
        .name(format!("block-{}", block.id))
        .spawn({
            let status_flag = status_flag.clone();
            move || {
                // remember in the thread-local the coordinate of this block
                COORD.with(|x| *x.borrow_mut() = Some(coord));
                do_work(
                    block,
                    coord,
                    #[cfg(feature = "actors")]
                    Some(status_flag),
                    #[cfg(not(feature = "actors"))]
                    None,
                );
            }
        })
        .unwrap();

    #[cfg(feature = "actors")]
    if metadata.network.config.use_actors() {
        tokio::spawn(async {
            use kameo::actor::Spawn;

            use crate::flowunits::actors::WorkerActor;

            WorkerActor::spawn(WorkerActor::new(status_flag))
        });
    }

    (join_handle, structure)
}

fn do_work<Op: Operator>(mut block: Block<Op>, coord: Coord, status_flag: Option<Arc<AtomicU8>>) {
    #[cfg(feature = "actors")]
    let catch_status_flag = status_flag.clone();
    let mut catch_panic = CatchPanic::new(move || {
        error!("worker {} crashed!", coord);
        #[cfg(feature = "actors")]
        if let Some(flag) = catch_status_flag {
            flag.store(
                WorkerStatus::Crashed as u8,
                std::sync::atomic::Ordering::SeqCst,
            );
        }
    });
    while !matches!(block.operators.next(), StreamElement::Terminate) {
        // nothing to do
    }
    catch_panic.defuse();
    #[cfg(feature = "actors")]
    if let Some(flag) = status_flag {
        flag.store(
            WorkerStatus::Completed as u8,
            std::sync::atomic::Ordering::SeqCst,
        );
    }
    info!("worker {} completed", coord);
}
