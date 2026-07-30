use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::{Sampler, TracerProvider as SdkTracerProvider};
use tracing_subscriber::{EnvFilter, Registry, prelude::*};

pub fn init_tracing(service_name: &'static str, otlp_endpoint: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .build()
        .unwrap();

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_sampler(Sampler::AlwaysOn)
        .with_resource(opentelemetry_sdk::Resource::new(vec![KeyValue::new(
            "service.name",
            service_name,
        )]))
        .build();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    let tracer = tracer_provider.tracer(service_name);
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let subscriber = Registry::default()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry_layer);

    tracing::subscriber::set_global_default(subscriber).unwrap();
}

pub fn shutdown_tracing() {
    opentelemetry::global::shutdown_tracer_provider();
}
