use std::{
    collections::HashMap,
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    cluster::{Cluster, Host},
    job_lookup::JobLookup,
    map::{MapRequest, MapResponse, handle_map},
    prime::{PrimeRequest, Programs, handle_prime},
    promote::{PromoteRequest, handle_promote},
    query::{QueryRequest, QueryResponse, handle_query},
    reduce::{ReduceRequest, handle_reduce},
    storage::Storage,
    wasm::WasmEnv,
};
use tarpc::context::{self, Context};
use tokio::sync::RwLock;

#[tarpc::service]
pub trait MapReduceService {
    /// Returns true if healthy, false otherwise.
    async fn heartbeat() -> bool;

    /// Resets the machine and stores the programs used by map and reduce.
    async fn prime(pr: PrimeRequest) -> Result<(), String>;

    async fn map(mp: MapRequest) -> Result<MapResponse, String>;

    async fn reduce(rr: ReduceRequest) -> Result<(), String>;

    async fn promote(pr: PromoteRequest) -> HashMap<String, Host>;

    async fn query(q: QueryRequest) -> Result<QueryResponse, String>;
}

#[derive(Clone)]
pub struct MapReduceServer<W: WasmEnv> {
    pub cluster: Arc<RwLock<Cluster>>,
    pub job_lookup: Arc<RwLock<JobLookup>>,

    /// The programs primed via the `prime` endpoint, used by map and reduce.
    pub programs: Arc<RwLock<Programs>>,

    pub wasm_env: W,

    pub storage: Arc<Storage>,
}

impl<W: WasmEnv> MapReduceServer<W> {
    pub fn new(
        cluster: Arc<RwLock<Cluster>>,
        job_lookup: Arc<RwLock<JobLookup>>,
        programs: Arc<RwLock<Programs>>,
        wasm_env: W,
        storage: Arc<Storage>,
    ) -> Self {
        Self {
            cluster,
            job_lookup,
            programs,
            wasm_env,
            storage,
        }
    }
}

impl<W: WasmEnv> MapReduceService for MapReduceServer<W> {
    async fn heartbeat(self, _: context::Context) -> bool {
        // If we can still accept heartbeat requests, that means we're healthy
        true
    }

    async fn prime(self, _: context::Context, pr: PrimeRequest) -> Result<(), String> {
        handle_prime(self, pr)
            .await
            .map_err(|e| format!("{}\n{:?}", e, e.source()))
    }

    async fn map(self, _: context::Context, mp: MapRequest) -> Result<MapResponse, String> {
        handle_map(self, mp)
            .await
            .map_err(|e| format!("{}\n{:?}", e, e.source()))
    }

    async fn reduce(self, _: context::Context, rr: ReduceRequest) -> Result<(), String> {
        handle_reduce(self, rr)
            .await
            .map_err(|e| format!("{}\n{:?}", e, e.source()))
    }

    async fn promote(self, _: context::Context, pr: PromoteRequest) -> HashMap<String, Host> {
        handle_promote(self, pr).await
    }

    async fn query(self, _: context::Context, q: QueryRequest) -> Result<QueryResponse, String> {
        handle_query(self, q)
            .await
            .map_err(|e| format!("{}\n{:?}", e, e.source()))
    }
}

/// Returns a request context with a longer default timeout. Useful as
/// map/reduce endpoints take a while loading WASM and computing output.
pub fn context() -> Context {
    let mut ctx = context::current();
    ctx.deadline = Instant::now() + Duration::from_secs(60);

    ctx
}
