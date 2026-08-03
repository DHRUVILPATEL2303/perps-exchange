use prometheus::{CounterVec, Encoder, Registry, TextEncoder, HistogramVec, Opts, HistogramOpts};
use std::sync::LazyLock;

pub static REGISTRY: LazyLock<Registry> = LazyLock::new(|| Registry::new());

pub static HTTP_REQUESTS_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    let opts = Opts::new("http_requests_total", "Total number of HTTP requests processed");
    let counter = CounterVec::new(opts, &["path", "method", "status"]).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

pub static HTTP_REQUEST_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("http_request_duration_seconds", "HTTP request latency in seconds");
    let histogram = HistogramVec::new(opts, &["path", "method"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static GRPC_REQUESTS_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    let opts = Opts::new("grpc_requests_total", "Total number of gRPC requests processed");
    let counter = CounterVec::new(opts, &["method", "status"]).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

pub static GRPC_REQUEST_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("grpc_request_duration_seconds", "gRPC request latency in seconds");
    let histogram = HistogramVec::new(opts, &["method"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static ORDERS_PROCESSED_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    let opts = Opts::new("orders_processed_total", "Total number of orders processed by the matching engine");
    let counter = CounterVec::new(opts, &["symbol", "status"]).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

pub static MATCHING_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("matching_duration_seconds", "Time taken to match an order in seconds");
    let histogram = HistogramVec::new(opts, &["symbol"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub fn gather_metrics() -> String {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
