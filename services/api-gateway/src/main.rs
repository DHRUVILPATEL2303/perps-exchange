use std::io::Result;
mod bootstrap;
mod state;
pub mod presentation;
pub mod application;
pub mod domain;
pub mod infrastructure;
#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();

    bootstrap::run().await
}
