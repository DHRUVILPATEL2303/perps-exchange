use std::io::Result;

pub mod bootstrap;
pub mod state;
pub mod domain;
pub mod infrastructure;
mod application;
#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();
    bootstrap::run().await
}
