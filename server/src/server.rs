use std::sync::Arc;

use crate::{
    cluster::ClusterConn,
    map::{MapRequest, MapResponse, handle_map},
    promote::{PromoteRequest, handle_promote},
    reduce::{ReduceRequest, handle_reduce},
    wasm::WasmEnv,
};
use tarpc::context;

#[tarpc::service]
pub trait MapReduceService {
    /// Returns true if healthy, false otherwise.
    async fn heartbeat() -> bool;

    async fn map(mp: MapRequest) -> MapResponse;

    async fn reduce(rr: ReduceRequest) -> ();

    async fn promote(pr: PromoteRequest) -> ();

    async fn download(file: String) -> Vec<u8>;
}

#[derive(Clone)]
pub struct MapReduceServer<W: WasmEnv> {
    cluster: Arc<ClusterConn>,
    wasm_env: W,
}

impl<W: WasmEnv> MapReduceServer<W> {
    pub fn new(cluster: ClusterConn) -> Self {
        let wasm_env = W::new().unwrap();
        Self {
            cluster: Arc::new(cluster),
            wasm_env,
        }
    }
}

impl<W: WasmEnv> MapReduceService for MapReduceServer<W> {
    async fn heartbeat(self, _: context::Context) -> bool {
        // If we can still accept heartbeat requests, that means we're healthy
        true
    }

    async fn map(self, _: context::Context, mp: MapRequest) -> MapResponse {
        handle_map(self.cluster.clone(), self.wasm_env.clone(), mp).await
    }

    async fn reduce(self, _: context::Context, _rr: ReduceRequest) -> () {
        handle_reduce().await
    }

    async fn promote(self, _: context::Context, pr: PromoteRequest) -> () {
        handle_promote(self.cluster.clone(), self.wasm_env.clone(), pr).await;
    }

    async fn download(self, _: context::Context, _file: String) -> Vec<u8> {
        vec![0x00]
    }
}
