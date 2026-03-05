use std::thread::JoinHandle;

use kameo::{
    actor::{RemoteActorRef, Spawn},
    prelude::Message,
    remote_message, Actor, RemoteActor,
};

use crate::{
    config::{MonitoringConfig, RemoteConfig},
    network::Coord,
    runner::HostExecutionResult,
    CoordUInt,
};

mod networking;

#[derive(Debug, Actor, RemoteActor)]
pub struct CoordinatorActor {
    supervisors: usize,
    completed: usize,
    config: MonitoringConfig,
    registered: Vec<(CoordUInt, RemoteActorRef<LocalCoordinator>)>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum SupervisorReport {
    Registration(CoordUInt, String),
    Heartbeat(CoordUInt),
    SupervisorCompleted(CoordUInt),
    WorkerCompleted(Coord),
    WorkerFailed(Coord, String),
}

#[remote_message]
impl Message<SupervisorReport> for CoordinatorActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: SupervisorReport,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match msg {
            SupervisorReport::Registration(coord, addr) => {
                log::info!("Supervisor registered for coord {} at {}", coord, addr);
                let supervisor_ref = RemoteActorRef::<LocalCoordinator>::lookup(&*addr)
                    .await
                    .unwrap()
                    .unwrap();
                self.registered.push((coord, supervisor_ref));
            }
            SupervisorReport::WorkerCompleted(coord) => {
                log::info!("Worker completed for coord {}", coord);
            }
            SupervisorReport::WorkerFailed(coord, error) => {
                log::error!("Worker failed for coord {}: {}", coord, error);
            }
            SupervisorReport::Heartbeat(host_id) => {
                log::info!("Received heartbeat from host {}", host_id);
            }
            SupervisorReport::SupervisorCompleted(host_id) => {
                log::info!("Supervisor on host {} reported completion", host_id);
                self.completed += 1;
                if self.completed >= self.supervisors {
                    log::info!("All supervisors reported completion, shutting down coordinator");
                    ctx.stop();
                }
            }
        }
    }
}

pub fn start_coordinator_service(
    monitoring: &MonitoringConfig,
    worker_count: usize,
) -> JoinHandle<HostExecutionResult> {
    log::info!(
        "Monitoring enabled: bind address {}, port {}, collection interval {}s",
        monitoring.bind_address,
        monitoring.port,
        monitoring.collection_interval
    );
    let monitoring = monitoring.clone();
    std::thread::Builder::new()
        .name("coordinator".to_string())
        .spawn(move || {
            let coordinator = CoordinatorActor {
                supervisors: worker_count,
                completed: 0,
                config: monitoring,
                registered: Vec::new(),
            };

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .unwrap();

            rt.block_on(async move {
                let listen_addr = format!(
                    "/ip4/{}/tcp/{}",
                    coordinator.config.bind_address, coordinator.config.port
                );
                log::info!("Bootstrapping coordinator service on {}", listen_addr);
                networking::bootstrap_coordinator(listen_addr.as_str())
                    .expect("Failed to bootstrap coordinator");
                let coordinator_ref = CoordinatorActor::spawn(coordinator);
                coordinator_ref
                    .register("coordinator")
                    .await
                    .expect("Failed to register coordinator actor");
                log::info!("Coordinator service started, waiting for shutdown");
                coordinator_ref.wait_for_shutdown().await;
            });
            log::info!("Coordinator service shutting down");

            Default::default()
        })
        .unwrap()
}

#[derive(Debug)]
pub struct LocalSupervisor {
    pub status_rx: flume::Receiver<crate::worker::WorkerStatus>,
    pub active_workers: usize,
    pub terminate_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub rt: tokio::runtime::Handle,
}

#[derive(Debug, Actor, RemoteActor)]
pub struct LocalCoordinator {
    pub terminate_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StopExecution;

impl Message<StopExecution> for LocalCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: StopExecution,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        log::info!("Received stop execution command, setting terminate flag");
        self.terminate_flag
            .store(true, std::sync::atomic::Ordering::SeqCst);
        ctx.stop();
    }
}

impl LocalSupervisor {
    pub fn start(self, config: &RemoteConfig) -> crate::worker::WorkerResult {
        log::info!(
            "Starting local supervisor with {} active workers",
            self.active_workers
        );
        let rx = self.status_rx.clone();
        let check_interval = config
            .monitoring
            .as_ref()
            .map(|m| m.collection_interval)
            .unwrap_or(10);
        let check_interval = std::time::Duration::from_secs(check_interval);
        let mut active_workers = self.active_workers;

        let monitoring = config
            .monitoring
            .clone()
            .expect("Failed to get monitoring configuration. Did you set it?");
        let coordinator_addr = format!(
            "/ip4/{}/tcp/{}",
            monitoring.coordinator_address, monitoring.port
        );
        log::info!(
            "Bootstrapping worker and connecting to coordinator at {}",
            coordinator_addr
        );
        networking::bootstrap_worker(coordinator_addr.as_str())
            .expect("Failed to bootstrap worker");

        let local_coordinator = LocalCoordinator {
            terminate_flag: self.terminate_flag.clone(),
        };
        let host_id = config.host_id.expect("Host id not configured");
        let supervisor_id = format!("supervisor-{}", host_id);
        let receiver_ref = LocalCoordinator::spawn(local_coordinator);

        self.rt.block_on(async move {
            receiver_ref.wait_for_startup().await;
            receiver_ref.register(&*supervisor_id).await.unwrap();
            log::info!("{} ready", supervisor_id);

            let coordinator_ref = {
                let mut attempts = 0u32;
                loop {
                    match RemoteActorRef::<CoordinatorActor>::lookup("coordinator").await {
                        Ok(Some(r)) => break r,
                        Ok(None) => {
                            let delay =
                                std::time::Duration::from_millis(500 * 2u64.pow(attempts.min(5)));
                            log::warn!(
                                "Coordinator actor not found (attempt {}), retrying in {:?}",
                                attempts + 1,
                                delay
                            );
                            tokio::time::sleep(delay).await;
                            attempts += 1;
                        }
                        Err(e) => {
                            let delay =
                                std::time::Duration::from_millis(500 * 2u64.pow(attempts.min(5)));
                            log::warn!(
                                "Coordinator lookup error (attempt {}): {e:?}, retrying in {:?}",
                                attempts + 1,
                                delay
                            );
                            tokio::time::sleep(delay).await;
                            attempts += 1;
                        }
                    }
                    if attempts > 5 {
                        panic!(
                            "Coordinator actor is not reachable after {} attempts",
                            attempts
                        );
                    }
                }
            };

            coordinator_ref
                .tell(&SupervisorReport::Registration(
                    host_id.clone(),
                    supervisor_id.clone(),
                ))
                .send()
                .expect("Failed to send registration message");

            log::info!("Found coordinator reference, entering monitoring loop");
            while receiver_ref.is_alive() {
                match tokio::time::timeout(check_interval, rx.recv_async()).await {
                    Ok(Ok(msg)) => {
                        coordinator_ref
                            .tell(&match msg {
                                crate::worker::WorkerStatus::Completed(coord) => {
                                    active_workers -= 1;
                                    SupervisorReport::WorkerCompleted(coord)
                                }
                                crate::worker::WorkerStatus::Failed(coord, error) => {
                                    SupervisorReport::WorkerFailed(coord, error)
                                }
                            })
                            .send()
                            .expect("Failed to send worker status");
                        if active_workers == 0 {
                            log::info!("All workers completed, shutting down local supervisor");
                            coordinator_ref
                                .ask(&SupervisorReport::SupervisorCompleted(host_id.clone()))
                                .await
                                .expect("Failed to send completion message to coordinator");
                            receiver_ref
                                .stop_gracefully()
                                .await
                                .expect("Failed to stop local coordinator actor");
                            break;
                        }
                    }
                    Ok(Err(_disconnected)) => break,
                    Err(_timeout) => {
                        coordinator_ref
                            .tell(&SupervisorReport::Heartbeat(host_id.clone()))
                            .send()
                            .expect("Failed to send heartbeat");
                    }
                }
            }
        });

        Ok(())
    }
}
