mod bootstrap;
mod infrastructure;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    telemetry::logging::init();
    bootstrap::run().await
}
