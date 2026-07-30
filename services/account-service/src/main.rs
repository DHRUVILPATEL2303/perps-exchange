use std::io::Result;

pub mod bootstrap;
pub mod state;
pub mod domain;
pub mod infrastructure;
pub mod grpc;

mod application;
mod presentation;
#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();
    bootstrap::run().await
}
