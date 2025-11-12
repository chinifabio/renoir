use std::sync::{Arc, atomic::AtomicU8};

use futures::StreamExt;
use kameo::{Actor, RemoteActor, Reply, actor::RemoteActorRef, prelude::Message, remote_message};
use log::debug;

#[derive(Debug, Reply, serde::Deserialize, serde::Serialize)]
pub(crate) enum WorkerStatus {
    Running = 0,
    Completed = 1,
    Crashed = 2,
    Unknown = 3,
}

impl WorkerStatus {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => WorkerStatus::Running,
            1 => WorkerStatus::Completed,
            2 => WorkerStatus::Crashed,
            _ => WorkerStatus::Unknown,
        }
    }
}

#[derive(Actor, RemoteActor)]
#[remote_actor(id = "worker")]
pub(crate) struct WorkerActor {
    status_flag: Arc<AtomicU8>,
}

impl WorkerActor {
    pub fn new(status_flag: Arc<AtomicU8>) -> Self {
        debug!("Creating new WorkerActor");
        Self {
            status_flag
        }
    }

    pub fn get_status(&self) -> WorkerStatus {
        let value = self.status_flag.load(std::sync::atomic::Ordering::SeqCst);
        let status = WorkerStatus::from_u8(value);
        debug!("WorkerActor status: {:?}", status);
        status
    }
}

#[derive(Actor, RemoteActor)]
pub(crate) struct SupervisorActor {}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Tick;

#[remote_message]
impl Message<Tick> for SupervisorActor {
    type Reply = bool;

    async fn handle(
        &mut self,
        _msg: Tick,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        debug!("SupervisorActor received Tick message");
        let mut lookup = RemoteActorRef::<WorkerActor>::lookup_all("worker");
        

        let mut alive = false;
        while let Some(worker_ref) = lookup.next().await {
            let worker_ref = worker_ref.expect("Failed to reach actor");
            let status = worker_ref.ask(&GetStatus).await.expect("Failed to get status");
            debug!("Worker status: {:?}", status);
            if matches!(status, WorkerStatus::Running) {
                alive = true;
            }
        }

        if !alive {
            debug!("All workers have completed or crashed. Supervisor exiting.");
            ctx.stop();
        }

        alive
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct GetStatus;

#[remote_message]
impl Message<GetStatus> for WorkerActor {
    type Reply = WorkerStatus;

    async fn handle(
        &mut self,
        _msg: GetStatus,
        _ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.get_status()
    }
}

