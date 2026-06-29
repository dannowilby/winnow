//! Fault tolerance of the leader coordination loop (`handle_promote`): a worker
//! dying at various points must not corrupt or stall the job.

use std::time::Duration;

use tokio::time::{sleep, timeout};

use mapreduce::promote::{PromoteRequest, handle_promote};

use crate::common::{
    TestNode, context_without_tracing, read_reduce_out, spawn_cluster, test_programs,
};

const ODD_SUM: i32 = 400; // 1 + 3 + ... + 39
const EVEN_SUM: i32 = 420; // 2 + 4 + ... + 40

/// A promote request over keys `"1".."4"`: one map job per key, the two parity
/// partitions.
fn promote_request() -> PromoteRequest {
    let programs = test_programs();
    PromoteRequest {
        read_src: programs.read_src,
        map_src: programs.map_src,
        reduce_src: programs.reduce_src,
        partition_src: programs.partition_src,
        m: 4,
        r: 2,
        keys: (1..=4).map(|k| k.to_string()).collect(),
    }
}

/// Reads the reduce output for `partition` off whichever node actually produced
/// it. Reassignment can move a partition's reduce away from its originally
/// recorded host, and only the node that finishes writes the output, so scan
/// them all.
fn reduce_output(nodes: &[TestNode], partition: &str) -> Vec<(String, i32)> {
    for node in nodes {
        if node
            .server
            .storage
            .get_reduce_out(partition.to_owned())
            .is_ok()
        {
            return read_reduce_out(&node.server, partition);
        }
    }
    panic!("no node produced reduce output for partition {partition}");
}

/// Asserts the job produced the full, correct totals for both partitions.
fn assert_correct_totals(nodes: &[TestNode]) {
    assert_eq!(
        reduce_output(nodes, "odd"),
        vec![("odd".to_owned(), ODD_SUM)]
    );
    assert_eq!(
        reduce_output(nodes, "even"),
        vec![("even".to_owned(), EVEN_SUM)]
    );
}

#[tokio::test]
async fn promote_completes_with_no_failures() {
    let (_net, nodes) = spawn_cluster("promote-baseline", 3).await;

    let reducing = handle_promote(
        nodes[0].server.clone(),
        context_without_tracing(),
        promote_request(),
    )
    .await;

    // Both partitions were reduced...
    assert_eq!(reducing.len(), 2);
    assert!(reducing.contains_key("odd"));
    assert!(reducing.contains_key("even"));
    // ...with correct totals.
    assert_correct_totals(&nodes);
}

#[tokio::test]
async fn completes_when_worker_dead_from_start() {
    let (_net, nodes) = spawn_cluster("promote-dead-from-start", 3).await;

    // Node 2 is gone before the run begins: priming fails for it, so it is
    // signalled failed and excluded from all job assignment. The survivors still
    // finish the job correctly.
    nodes[2].kill().await;

    let reducing = timeout(
        Duration::from_secs(120),
        handle_promote(
            nodes[0].server.clone(),
            context_without_tracing(),
            promote_request(),
        ),
    )
    .await
    .expect("promote should finish");

    assert_eq!(reducing.len(), 2);
    assert_correct_totals(&nodes);
}

#[tokio::test]
async fn survives_worker_death_mid_run() {
    let (_net, nodes) = spawn_cluster("promote-mid-run-failure", 3).await;

    let leader = nodes[0].server.clone();
    let request = promote_request();

    let reducing = timeout(Duration::from_secs(120), async {
        let (reducing, _) = tokio::join!(
            handle_promote(leader, context_without_tracing(), request),
            async {
                sleep(Duration::from_millis(100)).await;
                nodes[2].kill().await;
            }
        );
        reducing
    })
    .await
    .expect("promote should finish despite the failure");

    // The job converges: both partitions are accounted for and it returned
    // rather than hanging on the dead node.
    assert_eq!(reducing.len(), 2);
    assert!(reducing.contains_key("odd"));
    assert!(reducing.contains_key("even"));
}

#[tokio::test]
async fn recomputes_lost_map_output_for_reduce() {
    let (_net, nodes) = spawn_cluster("promote-recompute-map", 3).await;

    let leader = nodes[0].server.clone();
    let request = promote_request();

    let _reducing = timeout(Duration::from_secs(120), async {
        let (reducing, _) = tokio::join!(
            handle_promote(leader, context_without_tracing(), request),
            async {
                sleep(Duration::from_millis(100)).await;
                nodes[2].kill().await;
            }
        );
        reducing
    })
    .await;

    assert_correct_totals(&nodes);
}

#[tokio::test]
async fn leader_survives_when_all_workers_die() {
    let (_net, nodes) = spawn_cluster("promote-leader-survivor", 3).await;

    // Every non-leader node dies before the run. The leader serves itself over
    // loopback, so get_random falls back to it for every map and reduce job and
    // the pipeline still completes end to end on a single survivor.
    nodes[1].kill().await;
    nodes[2].kill().await;

    let reducing = timeout(
        Duration::from_secs(120),
        handle_promote(
            nodes[0].server.clone(),
            context_without_tracing(),
            promote_request(),
        ),
    )
    .await
    .expect("promote should finish on the sole survivor");

    assert_eq!(reducing.len(), 2);
    // All work ran on the leader, so its storage holds both partitions' output.
    assert_eq!(
        read_reduce_out(&nodes[0].server, "odd"),
        vec![("odd".to_owned(), ODD_SUM)]
    );
    assert_eq!(
        read_reduce_out(&nodes[0].server, "even"),
        vec![("even".to_owned(), EVEN_SUM)]
    );
}
