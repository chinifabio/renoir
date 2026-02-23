use crate::{network::Coord, worker::WorkerStatus};

pub(crate) mod execution_monitor;
pub(crate) mod job_manager;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) enum WorkerExecutionMessage {
    StatusUpdate(WorkerStatus),
    Heartbeat,
}

impl WorkerExecutionMessage {
    pub fn completed(coord: Coord) -> Self {
        Self::StatusUpdate(WorkerStatus::Completed(coord))
    }

    pub fn failed(coord: Coord, error: String) -> Self {
        Self::StatusUpdate(WorkerStatus::Failed(coord, error))
    }

    pub fn heartbeat() -> Self {
        Self::Heartbeat
    }
}