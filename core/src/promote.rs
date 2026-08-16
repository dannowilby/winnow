use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tarpc::context;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tracing::{Instrument, Span, error, info, info_span, warn, warn_span};

use crate::cluster::ActiveConnection;
use crate::server::{MapReduceServer, context, set_parent};
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
#[tracing::instrument(name = "Promote", skip_all)]
pub async fn handle_promote<W: WasmEnv>(
    server: MapReduceServer<W>,
    ctx: context::Context,
    pr: PromoteRequest,
) -> HashMap<String, Host> {
    let span = tracing::Span::current();
    set_parent(&span, &ctx);

    // verify that all connections are up to date
    server.cluster.write().await.reconnect().await;

    // prime every member: this clears their data folder + global state and
    // distributes the programs the map and reduce endpoints will use.
    prime_cluster(&server, &span, &pr).await;

    let mut events = JoinSet::new();
    spawn_initial_heartbeats(&mut events, &span, &server).await;

    let mut sent_initial_reduce_jobs = false;

    info!("Spawning map jobs");
    spawn_initial_map_jobs(&mut events, &span, &server, &pr).await;

    while let Some(Ok(event)) = events.join_next().await {
        match event {
            LeaderEvent::MachineFailure(host) => {
                handle_machine_failure(&mut events, &span, &server, &pr, host).await;
            }
            LeaderEvent::MapComplete(mr, host) => {
                let mut job_lookup = server.job_lookup.write().await;

                if job_lookup.try_get_host_by_index(mr.index) != Some(&host) {
                    warn!(
                        "dropping stale map completion for index {} from {:?}",
                        mr.index, host
                    );
                    continue;
                }

                job_lookup.complete_map_job(mr);

                let completed_map_jobs = job_lookup.progress.completed_map_jobs;
                let total_map_jobs = job_lookup.progress.total_map_jobs;
                drop(job_lookup);

                info!(
                    "Map job finished: {}/{}",
                    completed_map_jobs, total_map_jobs
                );

                if (completed_map_jobs < total_map_jobs) || sent_initial_reduce_jobs {
                    continue;
                }

                // spawn initial reduce jobs
                sent_initial_reduce_jobs = true;
                spawn_initial_reduce_jobs(&mut events, &span, &server).await;
            }
            LeaderEvent::ReduceComplete(partition, host) => {
                let mut job_lookup = server.job_lookup.write().await;

                if job_lookup.get_host_by_partition(&partition) != Some(&host) {
                    warn!(
                        "dropping stale reduce completion for partition {} from {:?}",
                        partition, host
                    );
                    continue;
                }

                info!("Reduce job completed on {:?}", host);
                job_lookup.complete_reduce_job(partition);

                let completed_reduce_jobs = job_lookup.progress.completed_reduce_jobs;
                let total_reduce_jobs = job_lookup.progress.total_reduce_jobs;
                drop(job_lookup);

                info!(
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
                events.spawn(
                    heartbeat(connection).instrument(info_span!(parent: &span, "heartbeat")),
                );
            }
        }
    }

    info!("Finished job");
    server.job_lookup.read().await.reducing.clone()
}

/// Sends a [PrimeRequest] to every live member so each machine wipes its data
/// folder + global state and stores the programs used by map and reduce.
async fn prime_cluster<W: WasmEnv>(server: &MapReduceServer<W>, span: &Span, pr: &PromoteRequest) {
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

            let failed = match result {
                Ok(Ok(())) => false,
                Ok(Err(e)) => {
                    error!("prime failed on host ({:?}): {}", connection.host, e);
                    true
                }
                Err(e) => {
                    error!(
                        "could not prime host ({:?}) at the transport level: {}",
                        connection.host, e
                    );
                    true
                }
            };

            if failed {
                server.cluster.write().await.signal_fail(connection.host);
            }
        }
        .instrument(info_span!(parent: span, "prime"))
    });

    join_all(prime_futures).await;
    server.job_lookup.write().await.signal_primed();
}

async fn spawn_initial_heartbeats<W: WasmEnv>(
    events: &mut JoinSet<LeaderEvent>,
    span: &Span,
    server: &MapReduceServer<W>,
) {
    // Start heartbeating the live cluster members
    for member in server.cluster.read().await.iter() {
        if member.client.is_none() {
            continue;
        }

        events.spawn(heartbeat(member.clone()).instrument(info_span!(parent: span, "heartbeat")));
    }
}

async fn spawn_initial_map_jobs<W: WasmEnv>(
    events: &mut JoinSet<LeaderEvent>,
    span: &Span,
    server: &MapReduceServer<W>,
    pr: &PromoteRequest,
) -> usize {
    info!("Spawning map jobs");

    let mut count = 0;

    // Store index + host and create request futures
    for (index, keys) in split_input_iter(pr.m, &pr.keys) {
        let mut cluster = server.cluster.write().await;
        let connection = cluster.get_random();

        server
            .job_lookup
            .write()
            .await
            .create_map_job(index, connection.host.clone());

        let request_future = send_map_request(connection.clone(), pr.r, index, keys.into())
            .instrument(info_span!(parent: span, "map_request", index));
        events.spawn(request_future);
        count += 1;
    }

    count
}

async fn spawn_initial_reduce_jobs<W: WasmEnv>(
    events: &mut JoinSet<LeaderEvent>,
    span: &Span,
    server: &MapReduceServer<W>,
) -> usize {
    info!("Spawning reduce jobs");

    let job_lookup = server.job_lookup.write().await;
    let partitions = job_lookup.partition.clone();
    drop(job_lookup);

    let count = partitions.len();

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
            partition.clone(),
            indices.iter().cloned().collect(),
        )
        .instrument(info_span!(parent: span, "reduce_request", partition = %partition));
        events.spawn(request_future);
    }

    count
}

/// Figures out which map/reduce jobs have failed and resubmits them to live
/// instances.
async fn handle_machine_failure<W: WasmEnv>(
    events: &mut JoinSet<LeaderEvent>,
    p_span: &Span,
    server: &MapReduceServer<W>,
    pr: &PromoteRequest,
    host: Host,
) {
    let span = warn_span!(parent: p_span, "Machine failure");

    let mut cluster = server.cluster.write().await;
    let mut job_lookup = server.job_lookup.write().await;

    error!("Host at {:?} failed", host.clone());

    cluster.signal_fail(host.clone());

    // Resend map jobs to new machines
    let lost_map_jobs = job_lookup.get_map_job_indices(host.clone());
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

        let request_future = send_map_request(connection.clone(), pr.r, *index, keys.into())
            .instrument(info_span!(parent: &span, "map_request", index = *index));
        events.spawn(request_future);
    }

    // Resend reduce jobs to new machines
    let lost_reduce_jobs = job_lookup
        .get_reduce_job_partitions(host.clone())
        .iter()
        .map(|v| (**v).clone())
        .collect::<Vec<String>>()
        .clone();
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
        )
        .instrument(info_span!(parent: &span, "reduce_request", partition = %partition));
        events.spawn(request_future);
    }
}

async fn heartbeat(connection: ActiveConnection) -> LeaderEvent {
    // The system seems to be getting swamped, a long timeout helps mitigate
    // false positive machine failures
    let heartbeat_timeout = 5;
    sleep(Duration::from_secs(heartbeat_timeout)).await;

    // if the connection has failed somehow already
    if connection.client.is_none() {
        return LeaderEvent::Heartbeat(connection);
    }

    let check_result = timeout(
        Duration::from_secs(heartbeat_timeout),
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
    r: u32,
    index: usize,
    keys: Vec<String>,
) -> LeaderEvent {
    let map_request_payload = MapRequest {
        index,
        key_range: keys,
        r,
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
                error!("{}", response.err().unwrap());
                return LeaderEvent::MachineFailure(connection.host.clone());
            }

            LeaderEvent::MapComplete(response.unwrap(), connection.host.clone())
        }
        Err(e) => {
            error!(
                "map request to {:?} failed at the transport level: {}",
                connection.host, e
            );
            LeaderEvent::MachineFailure(connection.host.clone())
        }
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
                error!("{}", response.err().unwrap());
                return LeaderEvent::MachineFailure(connection.host.clone());
            }

            LeaderEvent::ReduceComplete(partition, connection.host.clone())
        }
        Err(e) => {
            error!(
                "reduce request to {:?} failed at the transport level: {}",
                connection.host, e
            );
            LeaderEvent::MachineFailure(connection.host.clone())
        }
    }
}

fn chunk_bounds(n: usize, m: usize, index: usize) -> (usize, usize) {
    let quotient = n / m;
    let remainder = n % m;

    let start = index * quotient + index.min(remainder);
    let size = quotient + if index < remainder { 1 } else { 0 };

    (start, start + size)
}

fn split_input_iter(m: u32, key_list: &[String]) -> impl Iterator<Item = (usize, &[String])> {
    let n = key_list.len();
    let m = m as usize;
    let actual = m.min(n);

    (0..actual).map(move |index| {
        let (start, end) = chunk_bounds(n, m, index);
        (index, &key_list[start..end])
    })
}

fn get_split_at_index(m: u32, key_list: &[String], index: usize) -> &[String] {
    let (start, end) = chunk_bounds(key_list.len(), m as usize, index);
    &key_list[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(n: usize) -> Vec<String> {
        (0..n).map(|i| i.to_string()).collect()
    }

    #[test]
    fn split_input_iter_returns_correct_data() {
        // n=369, m=172: doesn't divide evenly, so sizes should differ by at
        // most one (25 chunks of 3, 147 chunks of 2) rather than the old
        // behavior of rounding every chunk up to 3 and only producing ~123.
        let key_list = keys(369);
        let chunks: Vec<(usize, &[String])> = split_input_iter(172, &key_list).collect();

        assert_eq!(chunks.len(), 172);

        let sizes: Vec<usize> = chunks.iter().map(|(_, c)| c.len()).collect();
        assert!(sizes.iter().all(|&s| s == 2 || s == 3));
        assert_eq!(sizes.iter().filter(|&&s| s == 3).count(), 25);

        // indices are sequential and every key is covered exactly once, in order
        let flattened: Vec<&String> = chunks
            .iter()
            .enumerate()
            .flat_map(|(expected_index, (index, c))| {
                assert_eq!(*index, expected_index);
                c.iter()
            })
            .collect();
        assert_eq!(flattened, key_list.iter().collect::<Vec<_>>());
    }

    #[test]
    fn split_input_iter_never_exceeds_available_keys() {
        // m=172 but only 5 keys available: should produce 5 single-key
        // chunks, not 172 (most of them empty).
        let key_list = keys(5);
        let chunks: Vec<(usize, &[String])> = split_input_iter(172, &key_list).collect();

        assert_eq!(chunks.len(), 5);
        assert!(chunks.iter().all(|(_, c)| c.len() == 1));
    }

    #[test]
    fn get_split_at_index_returns_correct_data() {
        let key_list = keys(369);
        let expected: Vec<(usize, &[String])> = split_input_iter(172, &key_list).collect();

        for (index, chunk) in expected {
            assert_eq!(get_split_at_index(172, &key_list, index), chunk);
        }
    }
}
