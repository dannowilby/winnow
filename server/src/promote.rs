use futures::{StreamExt, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use std::collections::HashSet;
use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};
use std::{future::Future, pin::Pin};
use tarpc::context;
use tokio::time::{sleep, timeout};

use crate::{
    cluster::{ClusterConn, Conn, Host},
    map::{MapRequest, MapResponse},
    reduce::ReduceRequest,
    wasm::WasmEnv,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct PromoteRequest {
    pub read_src: Vec<u8>,
    pub map_src: Vec<u8>,
    pub reduce_src: Vec<u8>,
    pub partition_src: Vec<u8>,

    pub m: u32,
    pub r: u32,

    pub keys: Vec<String>,
}

pub enum LeaderEvent {
    /// Key of the host
    MachineFailure(Host),

    /// (index of split, instant the job was created)
    MapComplete(MapResponse, Host),

    /// (partition identifier, instant the job was created)    
    ReduceComplete(String, Host),

    Heartbeat(Conn),
}

pub async fn handle_promote<W: WasmEnv>(
    cluster: Arc<ClusterConn>,
    _wasm_env: W,
    pr: PromoteRequest,
) {

    clean_stale_data();

    let mut events =
        JoinSet::new();

    for member in cluster.members.iter() {
        events.spawn(Box::pin(heartbeat(member.clone())));
    }

    for (index, keys) in split_input(pr.m, &pr.keys) {
        println!("Sending map job with {:?}", &keys);
        let req = map_request(
            cluster.get_modulo(index).clone(),
            MapRequest {
                index,
                key_range: keys.into(),
                r: pr.r,
                read_src: pr.read_src.clone(),
                map_src: pr.map_src.clone(),
                partition_src: pr.partition_src.clone(),
            },
        );

        events.spawn(Box::pin(req));
    }

    
    let mut completed_map_jobs = 0;

    let mut total_reduce_jobs = pr.r;
    let mut completed_reduce_jobs = 0;

    let mut partition_locations = HashMap::<String, HashSet<Host>>::new();

    while let Some(Ok(event)) = events.join_next().await {
        match event {
            LeaderEvent::MachineFailure(_host) => {}
            LeaderEvent::MapComplete(mr, host) => {
                for partition in mr.seen_partitions {
                    partition_locations
                        .entry(partition)
                        .and_modify(|v| { v.insert(host.clone()); })
                        .or_insert(HashSet::from([host.clone()]));
                }

                completed_map_jobs += 1;

                if completed_map_jobs < pr.m {
                    continue;
                }

                total_reduce_jobs = total_reduce_jobs.min(partition_locations.len() as u32);

                // we've completed all the map jobs
                partition_locations.iter().enumerate().for_each(
                    |(index, (partition, locations))| {
                        println!("Sending reduce job with {:?}, {:?}", &partition, &locations);
                        events.spawn(Box::pin(reduce_request(
                            cluster.get_modulo(index).clone(),
                            ReduceRequest {
                                partition: partition.clone(),
                                locations: locations.iter().cloned().collect::<Vec<_>>(),

                                reduce_src: pr.reduce_src.clone()
                            },
                        )));
                    },
                );
            }
            LeaderEvent::ReduceComplete(partition, host) => {
                println!("Reduced partition {} on {}.", partition, host.key());
                completed_reduce_jobs += 1;

                if completed_reduce_jobs >= total_reduce_jobs {
                    println!("Should be finishing job now.");
                    break;
                }
            }
            LeaderEvent::Heartbeat(conn) => {
                // requeue the heartbeat
                events.spawn(Box::pin(heartbeat(conn)));
            }
        }
    }

    println!("finished job!");
}

fn clean_stale_data() {
    let _ = std::fs::remove_dir_all("./data");
}

async fn heartbeat(conn: Conn) -> LeaderEvent {
    sleep(Duration::from_secs(1)).await;

    let check_result = timeout(Duration::from_secs(1), conn.1.heartbeat(context::current())).await;
    let alive = matches!(check_result, Ok(Ok(_)));

    match alive {
        true => LeaderEvent::Heartbeat(conn),
        false => LeaderEvent::MachineFailure(conn.0),
    }
}

async fn map_request(conn: Conn, mp: MapRequest) -> LeaderEvent {
    let result = conn.1.map(context::current(), mp).await;

    match result {
        Ok(mr) => LeaderEvent::MapComplete(mr, conn.0),
        Err(e) => {
            eprintln!("{}", e);
            LeaderEvent::MachineFailure(conn.0)
        }
    }
}

async fn reduce_request(conn: Conn, rr: ReduceRequest) -> LeaderEvent {
    let partition = rr.partition.clone();
    let result = conn.1.reduce(context::current(), rr).await;
    match result {
        Ok(_) => LeaderEvent::ReduceComplete(partition, conn.0),
        Err(e) => {
            eprintln!("{}", e);
            LeaderEvent::MachineFailure(conn.0)
        }
    }
}

pub fn split_input(
    m: u32,
    key_list: &[String],
) -> impl Iterator<Item = (usize, &[String])> {
    
    let n = key_list.len();
    let segments = n.div_ceil(m as usize);

    key_list.chunks(segments).enumerate()
}

pub fn get_input_split(_m: u32, _split_index: usize) {}
