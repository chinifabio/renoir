use std::sync::{Arc, atomic::AtomicBool};

use futures::TryStreamExt;
use kameo::{Actor, RemoteActor, Reply, actor::RemoteActorRef, prelude::Message, remote_message};

use crate::operator::end;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Ping{
    time: u64,
}

#[derive(Reply, serde::Serialize, serde::Deserialize)]
pub struct Pong {
    pub ended: bool,
    time: u64,
}

pub struct Tick;

#[derive(Actor, RemoteActor)]
pub struct MonitorActor {
    time: u64,
}

#[derive(Actor, RemoteActor)]
pub struct WorkerActor{
    pub(crate) end_flag: Arc<AtomicBool>,
}

impl Message<Tick> for MonitorActor {
    type Reply = bool;

    async fn handle(
        &mut self,
        _msg: Tick,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.time += 1;
        log::debug!("MonitorActor received Tick: time = {}", self.time);

        let mut workers = RemoteActorRef::<WorkerActor>::lookup_all("worker");
        let mut ended = true;
        while let Some(worker) = workers.try_next().await.unwrap() {
            log::debug!("Pinging worker actor...");
            let pong = worker.ask(&Ping {time: self.time}).await.unwrap();
            log::debug!("Worker actor ended: {}", pong.ended);
            if !pong.ended {
                ended = false;
            }
        }

        if ended {
            log::info!("All workers have ended. Stopping MonitorActor.");
            ctx.stop();
        }
        
        ended
    }
}

#[remote_message]
impl Message<Ping> for WorkerActor {
    type Reply = Pong;

    async fn handle(
        &mut self,
        msg: Ping,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let is_ended = self.end_flag.load(std::sync::atomic::Ordering::SeqCst);

        if is_ended {
            ctx.stop();
        }

        Pong {
            ended: is_ended,
            time: msg.time,
        }
    }
}

pub(crate) fn setup_actor_monitoring(monitor: bool) {
    let peer_id = kameo::remote::bootstrap().expect("Failed to bootstrap actor system");
    log::debug!("Actor cluster initialized: peer ID {}", peer_id);

    if !monitor {
        return;
    }

    let monitor_addr = MonitorActor::spawn(MonitorActor { time: 0 });
    tokio::spawn(async move {
        monitor_addr.register("monitor").await.expect("Failed to register monitor actor");
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            log::debug!("Sending ping to MonitorActor");
            let ended = monitor_addr.ask(Tick).await.unwrap();
            if ended {
                log::info!("MonitorActor has ended. Exiting monitoring loop.");
                break;
            }
        }
    });
    log::debug!("Monitor actor started");
}
