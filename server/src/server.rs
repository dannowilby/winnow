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
use opentelemetry::TraceFlags;
use opentelemetry::trace::{SpanContext, TraceContextExt, TraceState};
use tarpc::context::{self, Context};
use tokio::sync::RwLock;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

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

    async fn prime(self, ctx: context::Context, pr: PrimeRequest) -> Result<(), String> {
        handle_prime(self, ctx, pr)
            .await
            .map_err(|e| format!("{}\n{:?}", e, e.source()))
    }

    async fn map(self, ctx: context::Context, mp: MapRequest) -> Result<MapResponse, String> {
        handle_map(self, ctx, mp)
            .await
            .map_err(|e| format!("{}\n{:?}", e, e.source()))
    }

    async fn reduce(self, ctx: context::Context, rr: ReduceRequest) -> Result<(), String> {
        handle_reduce(self, ctx, rr)
            .await
            .map_err(|e| format!("{}\n{:?}", e, e.source()))
    }

    async fn promote(self, ctx: context::Context, pr: PromoteRequest) -> HashMap<String, Host> {
        handle_promote(self, ctx, pr).await
    }

    async fn query(self, ctx: context::Context, q: QueryRequest) -> Result<QueryResponse, String> {
        handle_query(self, ctx, q)
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

pub fn set_parent(span: &Span, ctx: &context::Context) {
    if !ctx.trace_id().is_none() {
        let trace = &ctx.trace_context;
        let parent_ctx = opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
            trace.trace_id.into(),
            trace.span_id.into(),
            TraceFlags::from(trace.sampling_decision),
            true,
            TraceState::default(),
        ));

        span.set_parent(parent_ctx);
    }
}
