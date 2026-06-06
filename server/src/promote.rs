use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

use crate::cluster::ActiveConnection;
use crate::server::{MapReduceServer, context};
use crate::{
    cluster::Host,
    map::{MapRequest, MapResponse},
    reduce::ReduceRequest,
    wasm::WasmEnv,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromoteRequest {
    pub read_src: Vec<u8>,
    pub map_src: Vec<u8>,
    pub reduce_src: Vec<u8>,
    pub partition_src: Vec<u8>,

    pub m: u32,
    pub r: u32,

    pub keys: Vec<String>,
}

/// When waiting for the various futures that the promote job creates to drive
/// the map-reduce process, a unified type needs to be resolvable.
pub enum LeaderEvent {
    MachineFailure(Host),

    MapComplete(MapResponse, Host),

    ReduceComplete(String, Host),

    Heartbeat(ActiveConnection),
}

/// Coordinates all the machines and drive map-reduce to completion.
pub async fn handle_promote<W: WasmEnv>(
    server: MapReduceServer<W>,
    pr: PromoteRequest,
) -> HashMap<String, Host> {
    // verify that all connections are up to date
    server.cluster.write().await.reconnect().await;

    // clean stale data
    let _ = std::fs::remove_dir_all("./data");
    server.job_lookup.write().await.clear();

    let mut events = JoinSet::new();
    spawn_initial_heartbeats(&mut events, &server).await;

    let mut completed_reduce_jobs = 0;
    let mut total_reduce_jobs = 0;

    let mut sent_initial_reduce_jobs = false;

    let mut completed_map_jobs = 0;
    let total_map_jobs = spawn_initial_map_jobs(&mut events, &server, &pr).await;

    while let Some(Ok(event)) = events.join_next().await {
        match event {
            LeaderEvent::MachineFailure(host) => {
                let (map_job_delta, reduce_job_delta) =
                    handle_machine_failure(&mut events, &server, &pr, host).await;

                completed_map_jobs -= map_job_delta;
                completed_reduce_jobs -= reduce_job_delta;
            }
            LeaderEvent::MapComplete(mr, _host) => {
                let mut job_lookup = server.job_lookup.write().await;
                job_lookup.complete_map_job(mr);
                drop(job_lookup);

                completed_map_jobs += 1;

                println!(
                    "Map job finished: {}/{}",
                    completed_map_jobs, total_map_jobs
                );

                if (completed_map_jobs < total_map_jobs) || sent_initial_reduce_jobs {
                    continue;
                }

                // spawn initial reduce jobs
                sent_initial_reduce_jobs = true;
                total_reduce_jobs = spawn_initial_reduce_jobs(&mut events, &server, &pr).await;
            }
            LeaderEvent::ReduceComplete(partition, _host) => {
                let mut job_lookup = server.job_lookup.write().await;
                job_lookup.complete_reduce_job(partition);

                completed_reduce_jobs += 1;

                println!(
                    "Completed reduce job: {}/{}",
                    completed_reduce_jobs, total_reduce_jobs
                );

                // Check if we're done
                if completed_reduce_jobs >= total_reduce_jobs {
                    break;
                }
            }
            LeaderEvent::Heartbeat(connection) => {
                // requeue the heartbeat
                events.spawn(heartbeat(connection));
            }
        }
    }

    server.job_lookup.read().await.reducing.clone()
}

async fn spawn_initial_heartbeats<W: WasmEnv>(
    events: &mut JoinSet<LeaderEvent>,
    server: &MapReduceServer<W>,
) {
    // Start heartbeating the live cluster members
    for member in server.cluster.read().await.iter() {
        if member.client.is_none() {
            continue;
        }

        events.spawn(heartbeat(member.clone()));
    }
}

async fn spawn_initial_map_jobs<W: WasmEnv>(
    events: &mut JoinSet<LeaderEvent>,
    server: &MapReduceServer<W>,
    pr: &PromoteRequest,
) -> usize {
    // Store index + host and create request futures
    for (index, keys) in split_input_iter(pr.m, &pr.keys) {
        let cluster = server.cluster.read().await;
        let connection = cluster.get_random();

        server
            .job_lookup
            .write()
            .await
            .create_map_job(index, connection.host.clone());

        let request_future = send_map_request(connection.clone(), pr.clone(), index, keys.into());
        events.spawn(request_future);
    }

    pr.m as usize
}

async fn spawn_initial_reduce_jobs<W: WasmEnv>(
    events: &mut JoinSet<LeaderEvent>,
    server: &MapReduceServer<W>,
    pr: &PromoteRequest,
) -> usize {
    println!("Spawning reduce jobs");

    let job_lookup = server.job_lookup.write().await;
    let partitions = job_lookup.partition.clone();
    drop(job_lookup);

    for (partition, indices) in partitions {
        let cluster = server.cluster.read().await;
        let connection = cluster.get_random().clone();
        let leader = cluster.get_loopback().host.clone();
        drop(cluster);

        server
            .job_lookup
            .write()
            .await
            .create_reduce_job(partition.clone(), connection.host.clone());

        let request_future = send_reduce_request(
            connection.clone(),
            pr.clone(),
            leader,
            partition,
            indices.clone(),
        );
        events.spawn(request_future);
    }

    pr.r as usize
}

/// Figures out which map/reduce jobs have failed and resubmits them to live
/// instances. Returns the number of jobs that failed
/// ie. (map job failures, reduce job failures)
async fn handle_machine_failure<W: WasmEnv>(
    events: &mut JoinSet<LeaderEvent>,
    server: &MapReduceServer<W>,
    pr: &PromoteRequest,
    host: Host,
) -> (usize, usize) {
    let mut cluster = server.cluster.write().await;
    let mut job_lookup = server.job_lookup.write().await;

    cluster.signal_fail(host.clone());

    // Resend map jobs to new machines
    let lost_map_jobs = job_lookup.get_map_job_indices(host.clone());
    let delta_map_jobs = lost_map_jobs.len();
    for index in &lost_map_jobs {
        let keys = get_split_at_index(pr.m, &pr.keys, *index);

        let connection = cluster.get_random();

        job_lookup.create_map_job(*index, connection.host.clone());

        let request_future = send_map_request(connection.clone(), pr.clone(), *index, keys.into());
        events.spawn(request_future);
    }

    // Resend reduce jobs to new machines
    let lost_reduce_jobs = job_lookup
        .get_reduce_job_partitions(host.clone())
        .iter()
        .map(|v| (**v).clone())
        .collect::<Vec<String>>()
        .clone();
    let delta_reduce_jobs = lost_reduce_jobs.len();
    for partition in lost_reduce_jobs {
        let indices = job_lookup.get_indices_for_partition(&partition).clone();

        let connection = cluster.get_random();

        job_lookup.create_reduce_job(partition.clone(), host.clone());

        let request_future = send_reduce_request(
            connection.clone(),
            pr.clone(),
            cluster.get_loopback().host.clone(),
            partition.clone(),
            indices,
        );
        events.spawn(request_future);
    }

    (delta_map_jobs, delta_reduce_jobs)
}

async fn heartbeat(connection: ActiveConnection) -> LeaderEvent {
    sleep(Duration::from_secs(1)).await;

    // if the connection has failed somehow already
    if connection.client.is_none() {
        return LeaderEvent::Heartbeat(connection);
    }

    let check_result = timeout(
        Duration::from_secs(5),
        connection.clone().client.unwrap().heartbeat(context()),
    )
    .await;

    match check_result {
        Ok(Ok(true)) => LeaderEvent::Heartbeat(connection),
        _ => LeaderEvent::MachineFailure(connection.host),
    }
}

/// Sends and awaits a map request to the connection. Any errors will resolve in
/// a [machine failure](crate::promote::LeaderEvent::MachineFailure).
async fn send_map_request(
    connection: ActiveConnection,
    pr: PromoteRequest,
    index: usize,
    keys: Vec<String>,
) -> LeaderEvent {
    let map_request_payload = MapRequest {
        index,
        key_range: keys,
        r: pr.r,
        read_src: pr.read_src.clone(),
        map_src: pr.map_src.clone(),
        partition_src: pr.partition_src.clone(),
    };

    if connection.client.is_none() {
        return LeaderEvent::Heartbeat(connection);
    }

    let result = connection
        .client
        .as_ref()
        .unwrap()
        .map(context(), map_request_payload)
        .await;

    match result {
        Ok(mr) => LeaderEvent::MapComplete(mr, connection.host.clone()),
        Err(_) => LeaderEvent::MachineFailure(connection.host.clone()),
    }
}

/// Sends and awaits a reduce request to the connection. Any errors will resolve in
/// a [machine failure](crate::promote::LeaderEvent::MachineFailure).
async fn send_reduce_request(
    connection: ActiveConnection,
    pr: PromoteRequest,
    leader: Host,
    partition: String,
    indices: Vec<usize>,
) -> LeaderEvent {
    let reduce_request_payload = ReduceRequest {
        partition: partition.clone(),
        indices,
        reduce_src: pr.reduce_src.clone(),
        leader,
    };

    if connection.client.is_none() {
        return LeaderEvent::Heartbeat(connection);
    }

    let result = connection
        .client
        .as_ref()
        .unwrap()
        .reduce(context(), reduce_request_payload)
        .await;

    match result {
        Ok(_) => LeaderEvent::ReduceComplete(partition, connection.host.clone()),
        Err(_) => LeaderEvent::MachineFailure(connection.host.clone()),
    }
}

pub fn split_input_iter(m: u32, key_list: &[String]) -> impl Iterator<Item = (usize, &[String])> {
    let n = key_list.len();
    let segments = n.div_ceil(m as usize);

    key_list.chunks(segments).enumerate()
}

pub fn get_split_at_index(m: u32, key_list: &[String], index: usize) -> &[String] {
    let n = key_list.len();
    let segments = n.div_ceil(m as usize);

    let start = index * segments;
    let end = (start + segments).min(n);

    &key_list[start..end]
}
