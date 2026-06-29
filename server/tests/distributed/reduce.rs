//! Download resilience of the reduce worker (`handle_reduce`), driven directly
//! against a multi-node cluster rather than through `handle_promote`.

use std::time::Duration;

use tokio::time::{sleep, timeout};

use mapreduce::{
    cluster::Host,
    map::MapResponse,
    reduce::{ReduceError, ReduceRequest, handle_reduce},
};

use crate::common::{
    TestNode, context_without_tracing, read_reduce_out, seed_map_output, spawn_cluster,
};

/// Records on the leader that `index` finished on `data_host` and produced
/// `partition`, so a reducer's `IsMapJobComplete` and `IndexLocation` queries
/// both resolve.
async fn seed_leader_index(leader: &TestNode, index: usize, partition: &str, data_host: Host) {
    let mut job_lookup = leader.server.job_lookup.write().await;
    job_lookup.create_map_job(index, data_host);
    job_lookup.complete_map_job(MapResponse {
        index,
        seen_partitions: vec![partition.to_owned()],
    });
}

/// A reduce request for `partition` over `indices`, coordinated by `leader`.
fn reduce_request(partition: &str, indices: Vec<usize>, leader: Host) -> ReduceRequest {
    ReduceRequest {
        partition: partition.to_owned(),
        indices,
        leader,
    }
}

/// The data host is unreachable when the reducer first tries to download, then
/// comes back. The reducer marks it failed, so each attempt must reconnect; the
/// index is requeued until the host returns, and the download then succeeds with
/// the correct total.
#[tokio::test]
async fn recovers_when_data_host_briefly_down() {
    let (net, nodes) = spawn_cluster("reduce-data-host-briefly-down", 3).await;

    // node1 is the data host, node0 the leader, node2 the reducer.
    seed_map_output(&nodes[1].server, 0, "odd", &[1, 3, 5]);
    seed_map_output(&nodes[1].server, 1, "odd", &[7, 9]);
    seed_leader_index(&nodes[0], 0, "odd", nodes[1].host.clone()).await;
    seed_leader_index(&nodes[0], 1, "odd", nodes[1].host.clone()).await;

    // Take the data host down and mark it failed in the reducer's view, so the
    // reduce loop has to reconnect (rather than reuse a stale client) to reach
    // it -- the only path that can actually recover once it returns.
    nodes[1].kill().await;
    nodes[2]
        .server
        .cluster
        .write()
        .await
        .signal_fail(nodes[1].host.clone());

    // Bring the data host back shortly after the reduce starts spinning.
    let revive_net = net.clone();
    let revive_host = nodes[1].host.clone();
    let revive_server = nodes[1].server.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(150)).await;
        revive_net.serve(revive_host, revive_server).await;
    });

    let result = timeout(
        Duration::from_secs(30),
        handle_reduce(
            nodes[2].server.clone(),
            context_without_tracing(),
            reduce_request("odd", vec![0, 1], nodes[0].host.clone()),
        ),
    )
    .await
    .expect("reduce should finish once the data host returns");
    result.expect("reduce should succeed");

    // 1 + 3 + 5 + 7 + 9
    assert_eq!(
        read_reduce_out(&nodes[2].server, "odd"),
        vec![("odd".to_owned(), 25)]
    );
}

#[tokio::test]
async fn requeues_on_query_failure() {
    let (_net, nodes) = spawn_cluster("reduce-query-failure", 3).await;

    // node1 is the data host, node0 the leader, node2 the reducer.
    seed_map_output(&nodes[1].server, 0, "odd", &[2, 4, 6]);

    // The leader knows the job finished (so the completeness guard passes) but
    // not yet *where* -- `IndexLocation` errors until we record the mapping.
    nodes[0]
        .server
        .job_lookup
        .write()
        .await
        .complete_map_job(MapResponse {
            index: 0,
            seen_partitions: vec!["odd".to_owned()],
        });

    let leader_lookup = nodes[0].server.job_lookup.clone();
    let data_host = nodes[1].host.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(200)).await;
        leader_lookup.write().await.create_map_job(0, data_host);
    });

    let result = timeout(
        Duration::from_secs(30),
        handle_reduce(
            nodes[2].server.clone(),
            context_without_tracing(),
            reduce_request("odd", vec![0], nodes[0].host.clone()),
        ),
    )
    .await
    .expect("reduce should finish once the leader records the index");
    result.expect("reduce should succeed");

    // 2 + 4 + 6
    assert_eq!(
        read_reduce_out(&nodes[2].server, "odd"),
        vec![("odd".to_owned(), 12)]
    );
}

/// The reducer must not download output for an index the leader has not yet
/// marked complete -- that output may be partial. The leader knows where the
/// index lives but not (yet) that it finished, so the reducer requeues until
/// completion is recorded, then downloads the full, correct total.
#[tokio::test]
async fn requeues_until_map_output_complete() {
    let (_net, nodes) = spawn_cluster("reduce-incomplete-map-output", 3).await;

    // node1 is the data host, node0 the leader, node2 the reducer.
    seed_map_output(&nodes[1].server, 0, "odd", &[1, 3, 5]);

    // The leader can locate the index (`IndexLocation` resolves) but has not
    // recorded its completion, so the guard rejects the download and requeues.
    nodes[0]
        .server
        .job_lookup
        .write()
        .await
        .create_map_job(0, nodes[1].host.clone());

    let leader_lookup = nodes[0].server.job_lookup.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(200)).await;
        leader_lookup.write().await.complete_map_job(MapResponse {
            index: 0,
            seen_partitions: vec!["odd".to_owned()],
        });
    });

    let result = timeout(
        Duration::from_secs(30),
        handle_reduce(
            nodes[2].server.clone(),
            context_without_tracing(),
            reduce_request("odd", vec![0], nodes[0].host.clone()),
        ),
    )
    .await
    .expect("reduce should finish once the map output is marked complete");
    result.expect("reduce should succeed");

    // 1 + 3 + 5
    assert_eq!(
        read_reduce_out(&nodes[2].server, "odd"),
        vec![("odd".to_owned(), 9)]
    );
}

/// The leader fully resolves the index, but the download itself fails because
/// the data host has no output written yet. The reducer requeues the index and
/// the download succeeds once the bytes land on the data host.
#[tokio::test]
async fn requeues_when_download_fails() {
    let (_net, nodes) = spawn_cluster("reduce-download-failure", 3).await;

    // node1 is the (live) data host, node0 the leader, node2 the reducer. The
    // leader says the index completed on node1, but node1 has nothing to serve
    // yet, so `DownloadMapOutput` errors until the output is written.
    seed_leader_index(&nodes[0], 0, "odd", nodes[1].host.clone()).await;

    let data_host = nodes[1].server.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(200)).await;
        seed_map_output(&data_host, 0, "odd", &[1, 3, 5]);
    });

    let result = timeout(
        Duration::from_secs(30),
        handle_reduce(
            nodes[2].server.clone(),
            context_without_tracing(),
            reduce_request("odd", vec![0], nodes[0].host.clone()),
        ),
    )
    .await
    .expect("reduce should finish once the data host has the output");
    result.expect("reduce should succeed");

    // 1 + 3 + 5
    assert_eq!(
        read_reduce_out(&nodes[2].server, "odd"),
        vec![("odd".to_owned(), 9)]
    );
}

/// A leader that is briefly unreachable is recovered: the reducer marks it
/// failed, reconnects within its retry budget once the leader returns, and
/// completes the job.
///
/// Ignored by default because the reconnect loop sleeps `2^retry` seconds, so it
/// pays at least one ~2s backoff before re-checking the connection.
#[tokio::test]
async fn recovers_when_leader_briefly_down() {
    let (net, nodes) = spawn_cluster("reduce-leader-briefly-down", 3).await;

    // node1 is the data host, node0 the leader, node2 the reducer.
    seed_map_output(&nodes[1].server, 0, "odd", &[1, 3, 5]);
    seed_leader_index(&nodes[0], 0, "odd", nodes[1].host.clone()).await;

    // Take the leader down and mark it failed in the reducer's view, so the
    // reduce loop must reconnect to it before it can query anything.
    nodes[0].kill().await;
    nodes[2]
        .server
        .cluster
        .write()
        .await
        .signal_fail(nodes[0].host.clone());

    // Bring the leader back while the reducer is still inside its retry budget.
    let revive_net = net.clone();
    let revive_host = nodes[0].host.clone();
    let revive_server = nodes[0].server.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(150)).await;
        revive_net.serve(revive_host, revive_server).await;
    });

    let result = timeout(
        Duration::from_secs(30),
        handle_reduce(
            nodes[2].server.clone(),
            context_without_tracing(),
            reduce_request("odd", vec![0], nodes[0].host.clone()),
        ),
    )
    .await
    .expect("reduce should finish once the leader returns");
    result.expect("reduce should succeed");

    // 1 + 3 + 5
    assert_eq!(
        read_reduce_out(&nodes[2].server, "odd"),
        vec![("odd".to_owned(), 9)]
    );
}

/// A leader that never comes back is unrecoverable: the reducer exhausts its
/// reconnect retries and gives up with a `ConnectionError`.
///
/// Ignored by default because it waits out the real exponential backoff
/// (2 + 4 + 8 = ~14s) before failing.
#[tokio::test]
async fn fails_when_leader_unrecoverable() {
    let (_net, nodes) = spawn_cluster("reduce-leader-unrecoverable", 3).await;

    // The leader is gone and marked failed in the reducer's view, so every
    // reconnect attempt fails and the index can never be located.
    nodes[0].kill().await;
    nodes[2]
        .server
        .cluster
        .write()
        .await
        .signal_fail(nodes[0].host.clone());

    let result = handle_reduce(
        nodes[2].server.clone(),
        context_without_tracing(),
        reduce_request("odd", vec![0], nodes[0].host.clone()),
    )
    .await;

    assert!(matches!(result, Err(ReduceError::ConnectionError(_))));
}
