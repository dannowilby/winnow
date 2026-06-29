use mapreduce::{
    cluster::Host,
    map::MapResponse,
    reduce::{ReduceRequest, handle_reduce},
};

use crate::common::{
    context_without_tracing, read_reduce_out, seed_map_output, test_served_node, test_server,
    write_intermediate,
};

/// The loopback host every [test_served_node] serves itself as.
fn loopback() -> Host {
    Host {
        domain: "[::1]".to_owned(),
        port: 0,
    }
}

#[tokio::test]
async fn reduces_single_index() {
    let server = test_served_node("reduce-single-index").await;
    let host = loopback();

    seed_map_output(&server, 0, "odd", &[1, 3, 5]);
    server
        .job_lookup
        .write()
        .await
        .create_map_job(0, host.clone());
    server
        .job_lookup
        .write()
        .await
        .complete_map_job(MapResponse {
            index: 0,
            seen_partitions: vec!["odd".to_owned()],
        });

    handle_reduce(
        server.clone(),
        context_without_tracing(),
        ReduceRequest {
            partition: "odd".to_owned(),
            indices: vec![0],
            leader: host,
        },
    )
    .await
    .expect("reduce succeeds");

    assert_eq!(read_reduce_out(&server, "odd"), vec![("odd".to_owned(), 9)]);
}

#[tokio::test]
async fn combines_values_across_indices() {
    let server = test_served_node("reduce-multiple-indices").await;
    let host = loopback();

    seed_map_output(&server, 0, "even", &[2, 4]);
    seed_map_output(&server, 1, "even", &[6, 8]);

    {
        let mut lookup = server.job_lookup.write().await;
        lookup.create_map_job(0, host.clone());
        lookup.create_map_job(1, host.clone());
        lookup.complete_map_job(MapResponse {
            index: 0,
            seen_partitions: vec!["even".to_owned()],
        });
        lookup.complete_map_job(MapResponse {
            index: 1,
            seen_partitions: vec!["even".to_owned()],
        });
    }

    handle_reduce(
        server.clone(),
        context_without_tracing(),
        ReduceRequest {
            partition: "even".to_owned(),
            indices: vec![0, 1],
            leader: host,
        },
    )
    .await
    .expect("reduce succeeds");

    assert_eq!(
        read_reduce_out(&server, "even"),
        vec![("even".to_owned(), 20)]
    );
}

#[tokio::test]
async fn groups_distinct_keys_in_partition() {
    let server = test_served_node("reduce-distinct-keys").await;
    let host = loopback();

    // The sorted fold should emit one record per key, ordered by key.
    write_intermediate(&server, 0, "p", "a", 1);
    write_intermediate(&server, 0, "p", "b", 10);
    write_intermediate(&server, 0, "p", "a", 2);

    server
        .job_lookup
        .write()
        .await
        .create_map_job(0, host.clone());

    server
        .job_lookup
        .write()
        .await
        .complete_map_job(MapResponse {
            index: 0,
            seen_partitions: vec!["p".to_owned()],
        });

    handle_reduce(
        server.clone(),
        context_without_tracing(),
        ReduceRequest {
            partition: "p".to_owned(),
            indices: vec![0],
            leader: host,
        },
    )
    .await
    .expect("reduce succeeds");

    assert_eq!(
        read_reduce_out(&server, "p"),
        vec![("a".to_owned(), 3), ("b".to_owned(), 10)]
    );
}

#[tokio::test]
async fn gracefully_exits_on_bad_wasm_binary() {
    let server = test_server("reduce-bad-wasm-binary").await;
    server.programs.write().await.reduce_src = b"not a wasm component".to_vec();

    let response = handle_reduce(
        server,
        context_without_tracing(),
        ReduceRequest {
            partition: "odd".to_owned(),
            indices: vec![],
            leader: loopback(),
        },
    )
    .await;

    assert!(response.is_err());
}
