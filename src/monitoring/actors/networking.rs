use futures::StreamExt;
use kameo::remote::{messaging, Behaviour};
use libp2p::{noise, swarm::SwarmEvent, tcp, yamux, Multiaddr, SwarmBuilder};

pub fn bootstrap_coordinator(listen_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            Ok(Behaviour::new(
                key.public().to_peer_id(),
                messaging::Config::default(),
            ))
        })?
        .build();

    // Initialize kameo global state
    swarm.behaviour().try_init_global()?;

    // Start listening for incoming Worker connections
    swarm.listen_on(listen_addr.parse()?)?;

    let local_peer_id = *swarm.local_peer_id();
    log::info!("Coordinator {} listening on {}", local_peer_id, listen_addr);

    // Drive the swarm in the background
    tokio::spawn(async move {
        loop {
            match swarm.next().await {
                Some(SwarmEvent::ConnectionEstablished {
                    peer_id, endpoint, ..
                }) => {
                    log::info!("Worker connected: {}", peer_id);
                    swarm.add_peer_address(peer_id, endpoint.get_remote_address().clone());
                }
                Some(SwarmEvent::ConnectionClosed { peer_id, .. }) => {
                    log::info!("Worker disconnected: {}", peer_id);
                }
                _ => {}
            }
        }
    });

    Ok(())
}

pub fn bootstrap_worker(coordinator_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            Ok(Behaviour::new(
                key.public().to_peer_id(),
                messaging::Config::default(),
            ))
        })?
        .build();

    swarm.behaviour().try_init_global()?;

    // Dial the coordinator
    let coordinator_multiaddr: Multiaddr = coordinator_addr.parse()?;
    swarm.dial(coordinator_multiaddr.clone())?;

    let local_peer_id = *swarm.local_peer_id();
    log::info!(
        "Worker {} dialing coordinator at {}",
        local_peer_id,
        coordinator_addr
    );

    tokio::spawn(async move {
        loop {
            match swarm.next().await {
                Some(SwarmEvent::ConnectionEstablished {
                    peer_id, endpoint, ..
                }) => {
                    log::info!("Worker connected to coordinator peer {}", peer_id);
                    swarm.add_peer_address(peer_id, endpoint.get_remote_address().clone());
                }
                Some(SwarmEvent::OutgoingConnectionError { error, .. }) => {
                    log::error!("Failed to connect to coordinator: {}", error);
                }
                _ => {}
            }
        }
    });

    Ok(())
}
