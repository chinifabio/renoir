//! Utility traits and structures related to the source operators.

pub use self::csv::*;
pub use self::csv_fast::*;
#[cfg(feature = "tokio")]
pub use async_stream::*;
pub use channel::*;
pub use file::*;
pub use iterator::*;
pub use parallel_iterator::*;

use crate::{
    block::Replication,
    operator::{Data, Operator},
};

#[cfg(feature = "tokio")]
mod async_stream;
mod channel;
mod csv;
mod file;
mod iterator;
mod parallel_iterator;
pub mod csv_fast;

/// This trait marks all the operators that can be used as sinks.
pub trait Source<Out: Data>: Operator<Out> {
    /// The maximum parallelism offered by this operator.
    fn replication(&self) -> Replication;
}
