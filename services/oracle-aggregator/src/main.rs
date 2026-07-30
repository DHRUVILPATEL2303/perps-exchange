mod domain;
mod application;
mod infrastructure;

mod state;
mod bootstrap;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();
    bootstrap::run().await
}
