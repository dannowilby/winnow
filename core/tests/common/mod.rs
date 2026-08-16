//! Test utils
#![allow(dead_code)]

mod net;

pub use net::InMemoryNet;
use tarpc::context::{self, Context};

use std::{
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use tokio::sync::{Mutex, RwLock, Semaphore};

use winnow_lib::{
    cluster::{ClusterList, Host},
    job_lookup::JobLookup,
    prime::Programs,
    server::MapReduceServer,
    storage::{IntermediateData, OutputData, Storage},
    wasm::{DefaultWasmEnv, WasmEnv},
};

/// Starts a server instance and registers it with the connector. The
/// [`InMemoryNet`] allows us to easily mock connection failures.
pub async fn add_node(net: Arc<InMemoryNet>, list: Vec<Host>, loopback: usize) {
    let loopback_host = list[loopback].clone();

    let cluster = ClusterList::from_hosts(list, loopback, net.clone())
        .connect()
        .await;

    let server = MapReduceServer::new(
        Arc::new(RwLock::new(cluster)),
        Arc::new(RwLock::new(JobLookup::new())),
        Arc::new(RwLock::new(Programs::default())),
        DefaultWasmEnv::new().expect("create wasm env"),
        Arc::new(Storage::new("./data")),
        test_wasm_slots(),
        test_reduce_locks(),
    );

    net.serve(loopback_host, server).await;
}

/// A generous cap for tests — plenty of headroom so it never gates test behavior.
fn test_wasm_slots() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(16))
}

fn test_reduce_locks() -> Arc<DashMap<String, Arc<Mutex<()>>>> {
    Arc::new(DashMap::new())
}

/// Loads the WASM components from `tests/data/` used by every test server.
pub fn test_programs() -> Programs {
    Programs {
        read_src: std::fs::read("./tests/data/read.wasm").expect("missing required test data"),
        map_src: std::fs::read("./tests/data/map.wasm").expect("missing required test data"),
        reduce_src: std::fs::read("./tests/data/reduce.wasm").expect("missing required test data"),
        partition_src: std::fs::read("./tests/data/partition.wasm")
            .expect("missing required test data"),
    }
}

/// Creates and prepares a unique on-disk storage root for a test.
fn test_storage(name: &str) -> Storage {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let storage_root = format!("/tmp/mapreduce-rs-map-test-{name}-{suffix}");
    std::fs::create_dir_all(&storage_root).expect("create test storage root");
    Storage::new(&storage_root)
}

/// Creates a single server instance
pub async fn test_server(name: &str) -> MapReduceServer<DefaultWasmEnv> {
    let net = Arc::new(InMemoryNet::new());
    let cluster = ClusterList::new(vec![("[::1]".to_owned(), 0)], 0, net)
        .connect()
        .await;

    MapReduceServer::new(
        Arc::new(RwLock::new(cluster)),
        Arc::new(RwLock::new(JobLookup::new())),
        Arc::new(RwLock::new(test_programs())),
        DefaultWasmEnv::new().expect("create wasm env"),
        Arc::new(test_storage(name)),
        test_wasm_slots(),
        test_reduce_locks(),
    )
}

/// Creates a single server that serves itself over the in-memory net and holds a
/// live connection to itself in its cluster.
pub async fn test_served_node(name: &str) -> MapReduceServer<DefaultWasmEnv> {
    // ext_sort spills to ./data; make sure it exists before reduce sorts.
    std::fs::create_dir_all("./data").expect("create sort tmp dir");

    let net = Arc::new(InMemoryNet::new());
    let host = Host {
        domain: "[::1]".to_owned(),
        port: 0,
    };

    let cluster = ClusterList::from_hosts(vec![host.clone()], 0, net.clone())
        .connect()
        .await;

    let server = MapReduceServer::new(
        Arc::new(RwLock::new(cluster)),
        Arc::new(RwLock::new(JobLookup::new())),
        Arc::new(RwLock::new(test_programs())),
        DefaultWasmEnv::new().expect("create wasm env"),
        Arc::new(test_storage(name)),
        test_wasm_slots(),
        test_reduce_locks(),
    );

    net.serve(host, server.clone()).await;

    // The cluster connected before the dialer was registered, so establish the
    // loopback connection now that dialing succeeds.
    server.cluster.write().await.reconnect().await;

    server
}

/// A node in a [`spawn_cluster`] cluster: the running server plus the handle
/// needed to kill it through the shared [`InMemoryNet`].
pub struct TestNode {
    pub server: MapReduceServer<DefaultWasmEnv>,
    pub host: Host,
    net: Arc<InMemoryNet>,
}

impl TestNode {
    /// Kill this node: its existing connections close and it stays unreachable,
    /// the in-memory twin of a crashed machine.
    pub async fn kill(&self) {
        self.net.kill(&self.host).await;
    }
}

/// Spins up an `n`-node cluster over a shared [`InMemoryNet`]. Every node serves
/// itself and holds live connections to all peers (including its own loopback)
pub async fn spawn_cluster(name: &str, n: usize) -> (Arc<InMemoryNet>, Vec<TestNode>) {
    // ext_sort spills to ./data; make sure it exists before any reduce sorts.
    std::fs::create_dir_all("./data").expect("create sort tmp dir");

    let net = Arc::new(InMemoryNet::new());
    let hosts: Vec<Host> = (0..n)
        .map(|i| Host {
            domain: "[::1]".to_owned(),
            port: i as u16,
        })
        .collect();

    let mut nodes = Vec::with_capacity(n);
    for (i, host) in hosts.iter().enumerate() {
        let cluster = ClusterList::from_hosts(hosts.clone(), i, net.clone())
            .connect()
            .await;

        let server = MapReduceServer::new(
            Arc::new(RwLock::new(cluster)),
            Arc::new(RwLock::new(JobLookup::new())),
            Arc::new(RwLock::new(test_programs())),
            DefaultWasmEnv::new().expect("create wasm env"),
            Arc::new(test_storage(&format!("{name}-node{i}"))),
            test_wasm_slots(),
            test_reduce_locks(),
        );

        net.serve(host.clone(), server.clone()).await;

        nodes.push(TestNode {
            server,
            host: host.clone(),
            net: net.clone(),
        });
    }

    // Every node's cluster connected before any dialer was registered, so all
    // those connections are dead. Now that every node serves, reconnect so each
    // holds live links to every peer (and its own loopback).
    for node in &nodes {
        node.server.cluster.write().await.reconnect().await;
    }

    (net, nodes)
}

/// Writes a single intermediate `(key, value)` record into `index`'s map output
/// for `partition`, encoding the value as an rmp `i32`
pub async fn write_intermediate(
    server: &MapReduceServer<DefaultWasmEnv>,
    index: usize,
    partition: &str,
    key: &str,
    value: i32,
) {
    server
        .storage
        .append_map_out(
            index,
            partition.to_owned(),
            IntermediateData {
                key: key.to_owned(),
                value: rmp_serde::to_vec(&value).expect("encode intermediate value"),
            },
        )
        .await
        .expect("write map output");
}

/// Seeds `index`'s map output for `partition` with one rmp-encoded `i32` record
/// per value, all under the partition name as the key. Convenience over
/// [write_intermediate] for the common single-key case; the reduce wasm sums the
/// values, so callers can assert the total.
///
/// Written as a single batch via [Storage::write_map_out], the same
/// write-temp-then-rename path the real map handler uses, so a concurrent
/// reader (e.g. tests racing a reducer against this seed call) always sees
/// either none of these records or all of them, never a partial file.
pub async fn seed_map_output(
    server: &MapReduceServer<DefaultWasmEnv>,
    index: usize,
    partition: &str,
    values: &[i32],
) {
    let records = values
        .iter()
        .map(|value| IntermediateData {
            key: partition.to_owned(),
            value: rmp_serde::to_vec(value).expect("encode intermediate value"),
        })
        .collect::<Vec<_>>();

    server
        .storage
        .write_map_out(index, partition, &records)
        .await
        .expect("write map output");
}

/// Decodes the intermediate map output based on the WASM components provided in `tests/data/`
pub async fn read_intermediate(
    server: &MapReduceServer<DefaultWasmEnv>,
    index: usize,
    partition: &str,
) -> Vec<(String, i32)> {
    let bytes = server
        .storage
        .get_map_out(index, partition.to_owned())
        .await
        .expect("read map output");
    let mut cursor = Cursor::new(bytes);
    let len = cursor.get_ref().len() as u64;
    let mut records = Vec::new();

    while cursor.position() < len {
        let data: IntermediateData =
            rmp_serde::from_read(&mut cursor).expect("decode intermediate record");
        let value = rmp_serde::from_slice::<i32>(&data.value).expect("decode intermediate value");
        records.push((data.key, value));
    }

    records
}

/// Decodes the reduce output for a partition into `(key, value)` pairs.
pub async fn read_reduce_out(
    server: &MapReduceServer<DefaultWasmEnv>,
    partition: &str,
) -> Vec<(String, i32)> {
    let bytes = server
        .storage
        .get_reduce_out(partition.to_owned())
        .await
        .expect("read reduce output");
    let mut cursor = Cursor::new(bytes);
    let len = cursor.get_ref().len() as u64;
    let mut records = Vec::new();

    while cursor.position() < len {
        let OutputData(key, value) =
            rmp_serde::from_read(&mut cursor).expect("decode output record");
        let value = rmp_serde::from_slice::<i32>(&value).expect("decode output value");
        records.push((key, value));
    }

    records
}

/// Returns a request context with a longer default timeout. Useful as
/// map/reduce endpoints take a while loading WASM and computing output.
pub fn context_without_tracing() -> Context {
    let mut ctx = context::current();
    ctx.deadline = Instant::now() + Duration::from_secs(60);

    ctx
}
