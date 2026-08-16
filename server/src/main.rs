#![recursion_limit = "256"]

use anyhow::{Context as _, Result};
use futures::prelude::*;
use mapreduce::{
    cluster::{ClusterList, Host},
    job_lookup::JobLookup,
    prime::Programs,
    server::{MapReduceServer, MapReduceService},
    storage::Storage,
    transport::TcpConnector,
    wasm::{DefaultWasmEnv, WasmEnv},
};
use serde::Deserialize;
use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};
use tarpc::server::{self, Channel, incoming::Incoming};
use tokio::sync::{RwLock, Semaphore};
use tracing::info;

/// Mirrors the structure of `cluster.json`.
#[derive(Debug, Deserialize)]
struct ClusterConfig {
    members: Vec<Host>,

    /// Whether to collect and export telemetry, defaults to `false`
    #[serde(default = "default_telemetry")]
    telemetry: bool,
}

fn default_telemetry() -> bool {
    true
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

/// Parses the cluster configuration file
fn load_cluster() -> Result<(ClusterList, (IpAddr, u16), bool)> {
    let config_path = std::env::var("CLUSTER_CONFIG").unwrap_or_else(|_| "cluster.json".to_owned());
    let config: ClusterConfig = serde_json::from_str(
        &fs::read_to_string(&config_path)
            .with_context(|| format!("failed to read cluster config at {config_path}"))?,
    )
    .with_context(|| format!("failed to parse cluster config at {config_path}"))?;

    let loopback = match std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
    {
        Some(port) => config
            .members
            .iter()
            .position(|host| host.port == port)
            .with_context(|| format!("PORT {port} does not match any cluster member"))?,
        None => 0,
    };

    let me = &config.members[loopback];
    let server_addr = (IpAddr::V4(Ipv4Addr::UNSPECIFIED), me.port);

    info!(
        "starting node {loopback} ({}:{}) of {} cluster member(s)",
        me.domain,
        me.port,
        config.members.len()
    );

    Ok((
        ClusterList::from_hosts(config.members, loopback, Arc::new(TcpConnector)),
        server_addr,
        config.telemetry,
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    let (cluster_list, server_addr, telemetry_enabled) = load_cluster()?;

    // init telemetry if enabled
    let telemetry = if telemetry_enabled {
        Some(mapreduce::telemetry::init("mapreduce-server")?)
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .init();
        None
    };

    let mut listener = tarpc::serde_transport::tcp::listen(
        &server_addr,
        tarpc::tokio_serde::formats::Bincode::default,
    )
    .await?;
    listener.config_mut().max_frame_length(usize::MAX);

    let cluster = Arc::new(RwLock::new(cluster_list.connect().await));
    let job_lookup = Arc::new(RwLock::new(JobLookup::new()));
    let programs = Arc::new(RwLock::new(Programs::default()));
    let wasm_env = DefaultWasmEnv::new().unwrap();
    let storage = Arc::new(Storage::new("./data"));

    // Caps concurrent WASM compile+run across tasks, preventing system overload
    let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
    let wasm_slots = Arc::new(Semaphore::new(cpus));
    let reduce_locks = Arc::new(dashmap::DashMap::new());

    listener
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        .max_channels_per_key(2, |t| t.transport().peer_addr().unwrap())
        .map(|channel| async {
            cluster.write().await.reconnect().await;
            let server = MapReduceServer::new(
                cluster.clone(),
                job_lookup.clone(),
                programs.clone(),
                wasm_env.clone(),
                storage.clone(),
                wasm_slots.clone(),
                reduce_locks.clone(),
            );
            channel.execute(server.serve()).for_each(spawn).await
        })
        .buffer_unordered(200)
        .for_each(|_| async {})
        .await;

    storage.clear().await?;

    if let Some(telemetry) = telemetry {
        telemetry.shutdown();
    }

    Ok(())
}
