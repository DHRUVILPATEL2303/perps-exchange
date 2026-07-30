mod application;
mod domain;
mod infrastructure;

mod bootstrap;
mod state;


use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();

    bootstrap::run().await
}
