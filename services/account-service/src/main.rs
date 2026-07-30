use std::io::Result;

pub mod bootstrap;
pub mod state;

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();
    bootstrap::run().await
}
