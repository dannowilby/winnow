use std::time::Duration;

use anyhow::{Context as _, Result};
use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    logs::SdkLoggerProvider,
    metrics::{PeriodicReader, SdkMeterProvider},
    propagation::TraceContextPropagator,
    trace::SdkTracerProvider,
};
use sysinfo::System;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

/// Default OTLP/gRPC endpoint
const DEFAULT_ENDPOINT: &str = "http://localhost:4317";

const METRIC_EXPORT_INTERVAL: Duration = Duration::from_secs(3);

pub struct Telemetry {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
    logger_provider: SdkLoggerProvider,
}

impl Telemetry {
    /// Flushes and shuts down every provider. Best-effort: shutdown errors are
    /// logged to stderr rather than propagated, since this runs during teardown.
    pub fn shutdown(self) {
        if let Err(e) = self.tracer_provider.shutdown() {
            eprintln!("failed to shut down tracer provider: {e}");
        }
        if let Err(e) = self.meter_provider.shutdown() {
            eprintln!("failed to shut down meter provider: {e}");
        }
        if let Err(e) = self.logger_provider.shutdown() {
            eprintln!("failed to shut down logger provider: {e}");
        }
    }
}

pub fn init(service_name: &'static str) -> Result<Telemetry> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned());
    let resource = resource(service_name);

    global::set_text_map_propagator(TraceContextPropagator::new());

    // --- Traces ---
    let span_exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
        .context("building OTLP span exporter")?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    // --- Metrics ---
    let metric_exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
        .context("building OTLP metric exporter")?;
    let reader = PeriodicReader::builder(metric_exporter)
        .with_interval(METRIC_EXPORT_INTERVAL)
        .build();
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(reader)
        .build();
    global::set_meter_provider(meter_provider.clone());

    // We don't take a handle; this causes the thread to run in the background
    // until the runtime is dropped.
    tokio::spawn(async {
        let sys_meter = global::meter("sys");
        let cpu_gauge = sys_meter.f64_gauge("cpu_usage").build();
        let mem_gauge = sys_meter.f64_gauge("mem_usage").build();

        let mut sys = System::new_all();
        sys.refresh_all();
        loop {
            sys.refresh_all();

            // record metrics
            let cpu_sum = sys
                .cpus()
                .iter()
                .map(|cpu| cpu.cpu_usage())
                .fold(0f64, |acc, e| acc + e as f64);

            let cpu_size = sys.cpus().len() as f64;

            let cpu_avg = cpu_sum / cpu_size;

            cpu_gauge.record(cpu_avg, &[]);

            let mem_usg = sys.used_memory() as f64 / sys.total_memory() as f64;

            mem_gauge.record(mem_usg, &[]);

            tokio::time::sleep(METRIC_EXPORT_INTERVAL).await;
        }
    });

    // --- Logs ---
    let log_exporter = LogExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
        .context("building OTLP log exporter")?;
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(log_exporter)
        .build();

    // --- `tracing` subscriber ---
    let trace_layer =
        tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer(service_name));
    let log_layer = OpenTelemetryTracingBridge::new(&logger_provider).with_filter(
        EnvFilter::new("info")
            .add_directive("hyper=off".parse().unwrap())
            .add_directive("tonic=off".parse().unwrap())
            .add_directive("h2=off".parse().unwrap())
            .add_directive("tower=off".parse().unwrap())
            .add_directive("reqwest=off".parse().unwrap()),
    );

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(trace_layer)
        .with(log_layer)
        .init();

    Ok(Telemetry {
        tracer_provider,
        meter_provider,
        logger_provider,
    })
}

/// Builds the resource describing this process: its service name plus an instance
/// id derived from the `PORT` env var so each cluster node is distinguishable.
fn resource(service_name: &'static str) -> Resource {
    Resource::builder()
        .with_service_name(service_name)
        // `service.instance.id` is the stable per-process id used to tell cluster
        // nodes apart in Grafana/Prometheus.
        .with_attribute(KeyValue::new("service.instance.id", instance_id()))
        .build()
}

/// A stable per-process identifier. Prefers an explicit override, then the node's
/// `PORT`, falling back to a default for one-off local runs.
fn instance_id() -> String {
    std::env::var("OTEL_SERVICE_INSTANCE_ID")
        .or_else(|_| std::env::var("PORT").map(|port| format!("node-{port}")))
        .unwrap_or_else(|_| "node-local".to_owned())
}
