mod bootstrap;
mod infrastructure;
mod grpc;
#[tokio::main]
async fn main() -> std::io::Result<()> {
    telemetry::logging::init();
    bootstrap::run().await
}
