use std::io::Result;

pub mod bootstrap;
pub mod state;
pub use sqlx;
pub mod application;
pub mod domain;
pub mod grpc;
pub mod infrastructure;
pub mod presentation;

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();
    bootstrap::run().await
}
