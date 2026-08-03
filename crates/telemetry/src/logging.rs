use tracing_subscriber::{EnvFilter, fmt};

pub fn init() {
    let service_name = std::env::var("SERVICE_NAME").unwrap_or_else(|_| "perps-service".to_string());
    
    if let Ok(endpoint) = std::env::var("OTLP_ENDPOINT") {
        // We must leak the string to pass it as &'static str to init_tracing
        let leaked_name: &'static str = Box::leak(service_name.into_boxed_str());
        crate::tracing::init_tracing(leaked_name, &endpoint);
    } else {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

        fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_file(true)
            .with_line_number(true)
            .init();
    }
}
