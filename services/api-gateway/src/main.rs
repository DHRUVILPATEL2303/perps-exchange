use std::io::Result;
pub mod application;
mod bootstrap;
pub mod domain;
pub mod infrastructure;
pub mod presentation;
mod state;
#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();

    bootstrap::run().await
}
