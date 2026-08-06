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
    let opts = HistogramOpts::new("matching_duration_seconds", "Time taken to match an order in seconds")
        .buckets(vec![
            0.000001, // 1 us
            0.000005, // 5 us
            0.000010, // 10 us
            0.000050, // 50 us
            0.000100, // 100 us
            0.000500, // 500 us
            0.001,    // 1 ms
            0.005,    // 5 ms
            0.010,    // 10 ms
        ]);
    let histogram = HistogramVec::new(opts, &["symbol"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static KAFKA_MESSAGES_CONSUMED_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    let opts = Opts::new("kafka_messages_consumed_total", "Total number of messages pulled from Kafka");
    let counter = CounterVec::new(opts, &["topic"]).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

pub static KAFKA_MESSAGES_PRODUCED_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    let opts = Opts::new("kafka_messages_produced_total", "Total number of messages published to Kafka");
    let counter = CounterVec::new(opts, &["topic"]).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

pub static TRADING_RISK_CHECK_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("trading_risk_check_duration_seconds", "Time taken for the Risk Engine to approve the order");
    let histogram = HistogramVec::new(opts, &["symbol"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static TRADING_DB_INSERT_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("trading_db_insert_duration_seconds", "Time taken to save the order in PostgreSQL");
    let histogram = HistogramVec::new(opts, &["symbol"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static TRADING_MARGIN_LOCK_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("trading_margin_lock_duration_seconds", "Time taken for the Account Service to lock the margin");
    let histogram = HistogramVec::new(opts, &["symbol"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static TRADING_KAFKA_PUBLISH_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("trading_kafka_publish_duration_seconds", "Time taken to produce the raw order to Kafka");
    let histogram = HistogramVec::new(opts, &["symbol"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static ORDER_TRANSIT_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("order_transit_duration_seconds", "Time taken for order to transit from trading-service to matching-engine via Kafka in seconds")
        .buckets(vec![
            0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0
        ]);
    let histogram = HistogramVec::new(opts, &["symbol"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static ORDER_MATCH_PURE_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("order_match_pure_duration_seconds", "Time taken for pure order book match execution in seconds")
        .buckets(vec![
            0.0000001, 0.0000002, 0.0000005, 0.000001, 0.000005, 0.000010, 0.000050, 0.000100, 0.000500, 0.001, 0.005, 0.010
        ]);
    let histogram = HistogramVec::new(opts, &["symbol"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static ORDER_CANCEL_PURE_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("order_cancel_pure_duration_seconds", "Time taken for pure order book cancel execution in seconds")
        .buckets(vec![
            0.0000001, 0.0000002, 0.0000005, 0.000001, 0.000005, 0.000010, 0.000050, 0.000100, 0.000500, 0.001, 0.005, 0.010
        ]);
    let histogram = HistogramVec::new(opts, &["symbol"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static ORDER_CHANNEL_LATENCY_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("order_channel_latency_seconds", "Time spent waiting in the worker queue in seconds")
        .buckets(vec![
            0.0000001, 0.0000002, 0.0000005, 0.000001, 0.000005, 0.000010, 0.000050, 0.000100, 0.000500, 0.001, 0.005, 0.010
        ]);
    let histogram = HistogramVec::new(opts, &["symbol"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static KAFKA_POLL_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("kafka_poll_duration_seconds", "Time taken to execute a poll/fetch call in seconds")
        .buckets(vec![
            0.000010, 0.000050, 0.000100, 0.000500, 0.001, 0.005, 0.010, 0.050, 0.100, 0.500
        ]);
    let histogram = HistogramVec::new(opts, &["topic"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static KAFKA_MESSAGES_PER_POLL: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("kafka_messages_per_poll", "Number of messages returned in a single poll batch")
        .buckets(vec![
            1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0
        ]);
    let histogram = HistogramVec::new(opts, &["topic"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static ORDER_DESERIALIZE_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("order_deserialize_duration_seconds", "Time taken to deserialize each order payload in seconds")
        .buckets(vec![
            0.0000001, 0.0000002, 0.0000005, 0.000001, 0.000005, 0.000010, 0.000050, 0.000100, 0.000500
        ]);
    let histogram = HistogramVec::new(opts, &["topic"]).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static PUBLISHING_ACK_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let opts = HistogramOpts::new("publishing_ack_duration_seconds", "Time taken to publish and receive ack for events (trade/depth) in seconds")
        .buckets(vec![
            0.000010, 0.000050, 0.000100, 0.000500, 0.001, 0.005, 0.010, 0.050, 0.100
        ]);
    let histogram = HistogramVec::new(opts, &["target"]).unwrap();
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
