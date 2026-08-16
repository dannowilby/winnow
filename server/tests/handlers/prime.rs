use mapreduce::{
    prime::{PrimeRequest, handle_prime},
    storage::{IntermediateData, OutputData},
};

use crate::common::{context_without_tracing, test_server};

fn prime_request() -> PrimeRequest {
    PrimeRequest {
        read_src: b"read".to_vec(),
        map_src: b"map".to_vec(),
        reduce_src: b"reduce".to_vec(),
        partition_src: b"partition".to_vec(),
    }
}

#[tokio::test]
async fn prime_resets_storage() {
    let server = test_server("prime-resets-storage").await;

    server
        .storage
        .append_map_out(
            1,
            "even".to_owned(),
            IntermediateData {
                key: "even".to_owned(),
                value: rmp_serde::to_vec(&2_i32).expect("encode value"),
            },
        )
        .await
        .expect("write map output");
    server
        .storage
        .append_reduce_out(
            "even".to_owned(),
            OutputData(
                "even".to_owned(),
                rmp_serde::to_vec(&30_i32).expect("encode value"),
            ),
        )
        .await
        .expect("write reduce output");

    handle_prime(server.clone(), context_without_tracing(), prime_request())
        .await
        .expect("prime succeeds");

    assert!(
        server
            .storage
            .get_map_out(1, "even".to_owned())
            .await
            .is_err()
    );
    assert!(
        server
            .storage
            .get_reduce_out("even".to_owned())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn prime_sets_programs() {
    let server = test_server("prime-sets-programs").await;
    let request = prime_request();

    handle_prime(server.clone(), context_without_tracing(), request.clone())
        .await
        .expect("prime succeeds");

    let programs = server.programs.read().await;
    assert_eq!(programs.read_src, request.read_src);
    assert_eq!(programs.map_src, request.map_src);
    assert_eq!(programs.reduce_src, request.reduce_src);
    assert_eq!(programs.partition_src, request.partition_src);
}
