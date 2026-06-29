use mapreduce::{
    cluster::Host,
    query::{QueryRequest, QueryResponse, handle_query},
    storage::{IntermediateData, OutputData},
};

use crate::common::{context_without_tracing, test_server};

#[tokio::test]
async fn returns_correct_map_output() {
    let server = test_server("query-map-output").await;
    let expected = IntermediateData {
        key: "even".to_owned(),
        value: rmp_serde::to_vec(&2_i32).expect("encode value"),
    };

    server
        .storage
        .append_map_out(4, "even".to_owned(), expected)
        .expect("write map output");
    let expected_bytes = server
        .storage
        .get_map_out(4, "even".to_owned())
        .expect("read map output");

    let response = handle_query(
        server,
        context_without_tracing(),
        QueryRequest::DownloadMapOutput(4, "even".to_owned()),
    )
    .await
    .expect("query succeeds");

    let QueryResponse::Data(data) = response else {
        panic!("expected data response");
    };
    assert_eq!(data, expected_bytes);
}

#[tokio::test]
async fn returns_correct_reduce_output() {
    let server = test_server("query-reduce-output").await;
    let expected = OutputData(
        "odd".to_owned(),
        rmp_serde::to_vec(&25_i32).expect("encode value"),
    );

    server
        .storage
        .append_reduce_out("odd".to_owned(), expected)
        .expect("write reduce output");
    let expected_bytes = server
        .storage
        .get_reduce_out("odd".to_owned())
        .expect("read reduce output");

    let response = handle_query(
        server,
        context_without_tracing(),
        QueryRequest::DownloadReduceOutput("odd".to_owned()),
    )
    .await
    .expect("query succeeds");

    let QueryResponse::Data(data) = response else {
        panic!("expected data response");
    };
    assert_eq!(data, expected_bytes);
}

#[tokio::test]
async fn returns_correct_index() {
    let server = test_server("query-index").await;
    let expected = Host {
        domain: "[::1]".to_owned(),
        port: 3000,
    };

    server
        .job_lookup
        .write()
        .await
        .create_map_job(8, expected.clone());

    let response = handle_query(
        server,
        context_without_tracing(),
        QueryRequest::IndexLocation(8),
    )
    .await
    .expect("query succeeds");

    let QueryResponse::Host(host) = response else {
        panic!("expected host response");
    };
    assert_eq!(host, expected);
}

#[tokio::test]
async fn gracefully_fails_on_invalid_map_output_query() {
    let server = test_server("query-missing-map-output").await;

    let response = handle_query(
        server,
        context_without_tracing(),
        QueryRequest::DownloadMapOutput(404, "missing".to_owned()),
    )
    .await;

    assert!(response.is_err());
}

#[tokio::test]
async fn gracefully_fails_on_invalid_reduce_output_query() {
    let server = test_server("query-missing-reduce-output").await;

    let response = handle_query(
        server,
        context_without_tracing(),
        QueryRequest::DownloadReduceOutput("missing".to_owned()),
    )
    .await;

    assert!(response.is_err());
}

#[tokio::test]
async fn gracefully_fails_on_invalid_index_query() {
    let server = test_server("query-missing-index").await;

    let response = handle_query(
        server,
        context_without_tracing(),
        QueryRequest::IndexLocation(404),
    )
    .await;

    assert!(response.is_err());
}
