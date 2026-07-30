use prometheus::{Counter, Encoder, Registry, TextEncoder};
use std::sync::LazyLock;

pub static REGISTRY: LazyLock<Registry> = LazyLock::new(|| Registry::new());

pub fn register_counter(name: &str, help: &str) -> Counter {
    let counter = Counter::new(name, help).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
}

pub fn gather_metrics() -> String {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
