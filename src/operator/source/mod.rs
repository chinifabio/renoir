//! Utility traits and structures related to the source operators.

pub use self::csv::*;
#[cfg(feature = "tokio")]
pub use async_stream::*;
#[cfg(feature = "avro")]
pub use avro::*;
pub use channel::*;
pub use file::*;
pub use iterator::*;
pub use parallel_iterator::*;

use crate::{block::Replication, operator::Operator};

#[cfg(feature = "tokio")]
mod async_stream;
#[cfg(feature = "avro")]
mod avro;
mod channel;
mod csv;
mod file;
mod iterator;
mod parallel_iterator;

/// This trait marks all the operators that can be used as sinks.
pub trait Source: Operator {
    /// The maximum parallelism offered by this operator.
    fn replication(&self) -> Replication;
}
