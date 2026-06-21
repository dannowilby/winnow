use futures::future::join_all;
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
    prime::PrimeRequest,
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

pub enum PromoteError {}

/// Coordinates all the machines and drive map-reduce to completion.
pub async fn handle_promote<W: WasmEnv>(
    server: MapReduceServer<W>,
    pr: PromoteRequest,
) -> HashMap<String, Host> {
    // verify that all connections are up to date
    server.cluster.write().await.reconnect().await;

    // prime every member: this clears their data folder + global state and
    // distributes the programs the map and reduce endpoints will use.
    prime_cluster(&server, &pr).await;

    let mut events = JoinSet::new();
    spawn_initial_heartbeats(&mut events, &server).await;

    let mut completed_reduce_jobs: usize = 0;
    let mut total_reduce_jobs: usize = 0;

    let mut sent_initial_reduce_jobs = false;

    let mut completed_map_jobs: usize = 0;
    println!("Spawning map jobs");
    let total_map_jobs: usize = spawn_initial_map_jobs(&mut events, &server, &pr).await;

    while let Some(Ok(event)) = events.join_next().await {
        match event {
            LeaderEvent::MachineFailure(host) => {
                let (map_job_delta, reduce_job_delta) =
                    handle_machine_failure(&mut events, &server, &pr, host).await;

                completed_map_jobs = completed_map_jobs.saturating_sub(map_job_delta);
                completed_reduce_jobs = completed_reduce_jobs.saturating_sub(reduce_job_delta);
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
            LeaderEvent::ReduceComplete(partition, host) => {
                println!("[INFO]: Reduce job completed on {:?}", host);
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

    println!("Finished job");
    server.job_lookup.read().await.reducing.clone()
}

/// Sends a [PrimeRequest] to every live member so each machine wipes its data
/// folder + global state and stores the programs used by map and reduce.
async fn prime_cluster<W: WasmEnv>(server: &MapReduceServer<W>, pr: &PromoteRequest) {
    let prime_request = PrimeRequest {
        read_src: pr.read_src.clone(),
        map_src: pr.map_src.clone(),
        reduce_src: pr.reduce_src.clone(),
        partition_src: pr.partition_src.clone(),
    };

    let connections = server
        .cluster
        .read()
        .await
        .iter()
        .filter(|connection| connection.client.is_some())
        .cloned()
        .collect::<Vec<_>>();

    let prime_futures = connections.into_iter().map(|connection| {
        let prime_request = prime_request.clone();
        async move {
            let result = connection
                .client
                .as_ref()
                .unwrap()
                .prime(context(), prime_request)
                .await;

            if result.is_err() {
                println!(
                    "[prime][WARNING]: could not prime host ({:?}), see below for details",
                    connection.host
                );
                println!("[prime][WARNING]: {}", result.err().unwrap());
                server.cluster.write().await.signal_fail(connection.host);
            }
        }
    });

    join_all(prime_futures).await;
    server.job_lookup.write().await.signal_primed();
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
        let mut cluster = server.cluster.write().await;
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
        let mut cluster = server.cluster.write().await;
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
            leader,
            partition,
            indices.iter().cloned().collect(),
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

    println!("[WARNING]: Host at {:?} failed", host.clone());

    cluster.signal_fail(host.clone());

    // Resend map jobs to new machines
    let lost_map_jobs = job_lookup.get_map_job_indices(host.clone());
    let delta_map_jobs = lost_map_jobs.len();
    for index in &lost_map_jobs {
        // check if this map job is already running somewhere else
        if job_lookup.get_host_by_index(*index) != &host {
            // if it is, just move on to the next
            continue;
        }

        let keys = get_split_at_index(pr.m, &pr.keys, *index);

        let connection = cluster.get_random();
        job_lookup.signal_map_job_failure(*index);
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
        // check if we've already requeued the reduce job
        if job_lookup.get_host_by_partition(&partition).unwrap() != &host {
            continue;
        }

        let indices = job_lookup.get_indices_for_partition(&partition).clone();

        let connection = cluster.get_random();

        job_lookup.signal_reduce_job_failure(&partition);
        job_lookup.create_reduce_job(partition.clone(), connection.host.clone());

        let request_future = send_reduce_request(
            connection.clone(),
            cluster.get_loopback().host.clone(),
            partition.clone(),
            indices.iter().cloned().collect(),
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
        Ok(response) => {
            if response.is_err() {
                println!("[map][WARNING]: {}", response.err().unwrap());
                return LeaderEvent::MachineFailure(connection.host.clone());
            }

            LeaderEvent::MapComplete(response.unwrap(), connection.host.clone())
        }
        Err(_) => LeaderEvent::MachineFailure(connection.host.clone()),
    }
}

/// Sends and awaits a reduce request to the connection. Any errors will resolve in
/// a [machine failure](crate::promote::LeaderEvent::MachineFailure).
async fn send_reduce_request(
    connection: ActiveConnection,
    leader: Host,
    partition: String,
    indices: Vec<usize>,
) -> LeaderEvent {
    let reduce_request_payload = ReduceRequest {
        partition: partition.clone(),
        indices,
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
        Ok(response) => {
            if response.is_err() {
                println!("[reduce][WARNING]: {}", response.err().unwrap());
                return LeaderEvent::MachineFailure(connection.host.clone());
            }

            LeaderEvent::ReduceComplete(partition, connection.host.clone())
        }
        Err(_) => LeaderEvent::MachineFailure(connection.host.clone()),
    }
}

fn split_input_iter(m: u32, key_list: &[String]) -> impl Iterator<Item = (usize, &[String])> {
    let n = key_list.len();
    let segments = n.div_ceil(m as usize);

    key_list.chunks(segments).enumerate()
}

fn get_split_at_index(m: u32, key_list: &[String], index: usize) -> &[String] {
    let n = key_list.len();
    let segments = n.div_ceil(m as usize);

    let start = index * segments;
    let end = (start + segments).min(n);

    &key_list[start..end]
}

#[cfg(test)]
mod tests {

    #[test]
    fn split_input_iter_returns_correct_data() {}

    #[test]
    fn get_split_at_index_returns_correct_data() {}
}
