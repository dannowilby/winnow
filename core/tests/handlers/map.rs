use winnow_lib::map::{MapRequest, handle_map};

use crate::common::{context_without_tracing, read_intermediate, test_server};

#[tokio::test]
async fn deduplicates_partitions() {
    let server = test_server("deduplicates-partitions").await;

    let response = handle_map(
        server,
        context_without_tracing(),
        MapRequest {
            index: 7,
            key_range: vec!["1".to_owned(), "2".to_owned()],
            r: 4,
        },
    )
    .await
    .expect("map succeeds");

    assert_eq!(response.seen_partitions.len(), 2);
    assert!(response.seen_partitions.contains(&"even".to_owned()));
    assert!(response.seen_partitions.contains(&"odd".to_owned()));
}

#[tokio::test]
async fn generates_correct_response() {
    let server = test_server("generates-correct-response").await;

    let response = handle_map(
        server,
        context_without_tracing(),
        MapRequest {
            index: 42,
            key_range: vec!["1".to_owned()],
            r: 2,
        },
    )
    .await;

    assert!(response.is_ok());
}

#[tokio::test]
async fn writes_correct_data() {
    let server = test_server("writes-correct-data").await;

    handle_map(
        server.clone(),
        context_without_tracing(),
        MapRequest {
            index: 3,
            key_range: vec!["1".to_owned()],
            r: 2,
        },
    )
    .await
    .expect("map succeeds");

    assert_eq!(
        read_intermediate(&server, 3, "odd").await,
        vec![
            ("odd".to_owned(), 1),
            ("odd".to_owned(), 3),
            ("odd".to_owned(), 5),
            ("odd".to_owned(), 7),
            ("odd".to_owned(), 9),
        ]
    );
    assert_eq!(
        read_intermediate(&server, 3, "even").await,
        vec![
            ("even".to_owned(), 2),
            ("even".to_owned(), 4),
            ("even".to_owned(), 6),
            ("even".to_owned(), 8),
            ("even".to_owned(), 10),
        ]
    );
}

#[tokio::test]
async fn gracefully_exits_on_bad_wasm_binary() {
    let server = test_server("bad-wasm-binary").await;
    server.programs.write().await.map_src = b"not a wasm component".to_vec();

    let response = handle_map(
        server,
        context_without_tracing(),
        MapRequest {
            index: 9,
            key_range: vec!["1".to_owned()],
            r: 2,
        },
    )
    .await;

    assert!(response.is_err());
}

#[tokio::test]
async fn gracefully_exits_on_bad_wasm_call() {
    let server = test_server("bad-wasm-call").await;

    let response = handle_map(
        server,
        context_without_tracing(),
        MapRequest {
            index: 10,
            key_range: vec!["not-a-number".to_owned()],
            r: 2,
        },
    )
    .await;

    assert!(response.is_err());
}
