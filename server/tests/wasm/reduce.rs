//! Verifies the async component-model path: load the real reduce component and
//! drive its now-`async` export via `.await`, confirming the host correctly
//! runs an async-lifted guest export.

use mapreduce::wasm::{DefaultWasmEnv, WasmEnv, handle::reduce::ReduceFn};

#[tokio::test]
async fn reduce_component_runs_async() {
    let binary = std::fs::read("./tests/data/reduce.wasm")
        .expect("build components first: `just build-components`");

    let env = DefaultWasmEnv::new().expect("create wasm env");
    let mut reducer = env.load_reduce_binary(&binary).await.expect("load reduce");

    // First fold: empty accumulator + value 5 => 5
    let acc = reducer
        .reduce("k", &rmp_serde::to_vec(&5_i32).unwrap(), &[])
        .await
        .expect("reduce call 1");
    assert_eq!(rmp_serde::from_slice::<i32>(&acc).unwrap(), 5);

    // Second fold: acc 5 + value 3 => 8
    let acc = reducer
        .reduce("k", &rmp_serde::to_vec(&3_i32).unwrap(), &acc)
        .await
        .expect("reduce call 2");
    assert_eq!(rmp_serde::from_slice::<i32>(&acc).unwrap(), 8);
}
