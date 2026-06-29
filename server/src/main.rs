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
    net::{IpAddr, Ipv6Addr},
    sync::Arc,
};
use tarpc::server::{self, Channel, incoming::Incoming};
use tokio::sync::RwLock;
use tracing::info;

/// Mirrors the structure of `cluster.json`.
#[derive(Debug, Deserialize)]
struct ClusterConfig {
    members: Vec<Host>,
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

/// Reads `cluster.json`, works out which member *this* process is, and returns
/// the resulting cluster list together with the address it should bind to.
fn load_cluster() -> Result<(ClusterList, (IpAddr, u16))> {
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
    let server_addr = (IpAddr::V6(Ipv6Addr::LOCALHOST), me.port);

    info!(
        "starting node {loopback} ({}:{}) of {} cluster member(s)",
        me.domain,
        me.port,
        config.members.len()
    );

    Ok((
        ClusterList::from_hosts(config.members, loopback, Arc::new(TcpConnector)),
        server_addr,
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    let telemetry = mapreduce::telemetry::init("mapreduce-server")?;

    let (cluster_list, server_addr) = load_cluster()?;

    let mut listener = tarpc::serde_transport::tcp::listen(
        &server_addr,
        tarpc::tokio_serde::formats::Json::default,
    )
    .await?;
    listener.config_mut().max_frame_length(usize::MAX);

    let cluster = Arc::new(RwLock::new(cluster_list.connect().await));
    let job_lookup = Arc::new(RwLock::new(JobLookup::new()));
    let programs = Arc::new(RwLock::new(Programs::default()));
    let wasm_env = DefaultWasmEnv::new().unwrap();
    let storage = Arc::new(Storage::new("./data"));

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
            );
            channel.execute(server.serve()).for_each(spawn).await
        })
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;

    storage.clear()?;

    telemetry.shutdown();

    Ok(())
}
