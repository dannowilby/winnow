use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    cluster::{Cluster, Host},
    job_lookup::{self, JobLookup},
    map::{MapRequest, MapResponse, handle_map},
    promote::{PromoteRequest, handle_promote},
    query::{QueryRequest, QueryResponse, handle_query},
    reduce::{ReduceRequest, handle_reduce},
    wasm::WasmEnv,
};
use tarpc::context::{self, Context};
use tokio::sync::RwLock;

#[tarpc::service]
pub trait MapReduceService {
    /// Returns true if healthy, false otherwise.
    async fn heartbeat() -> bool;

    async fn map(mp: MapRequest) -> MapResponse;

    async fn reduce(rr: ReduceRequest) -> ();

    async fn promote(pr: PromoteRequest) -> HashMap<String, Host>;

    async fn query(q: QueryRequest) -> QueryResponse;
}

#[derive(Clone)]
pub struct MapReduceServer<W: WasmEnv> {
    pub cluster: Arc<RwLock<Cluster>>,
    pub job_lookup: Arc<RwLock<JobLookup>>,

    pub wasm_env: W,
}

impl<W: WasmEnv> MapReduceServer<W> {
    pub fn new(
        cluster: Arc<RwLock<Cluster>>,
        job_lookup: Arc<RwLock<JobLookup>>,
        wasm_env: W,
    ) -> Self {
        Self {
            cluster,
            job_lookup,
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
        handle_map(self.wasm_env.clone(), mp).await
    }

    async fn reduce(self, _: context::Context, rr: ReduceRequest) -> () {
        handle_reduce(self, rr).await
    }

    async fn promote(self, _: context::Context, pr: PromoteRequest) -> HashMap<String, Host> {
        handle_promote(self, pr).await
    }

    async fn query(self, _: context::Context, q: QueryRequest) -> QueryResponse {
        handle_query(self, q).await
    }
}

/// Returns a request context with a longer default timeout. Useful as
/// map/reduce endpoints take a while loading WASM and computing output.
pub fn context() -> Context {
    let mut ctx = context::current();
    ctx.deadline = Instant::now() + Duration::from_secs(60);

    ctx
}
