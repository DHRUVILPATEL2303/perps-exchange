use std::io::Result;

pub mod bootstrap;
pub mod state;
pub use sqlx;
pub mod application;
pub mod domain;
pub mod infrastructure;
pub mod presentation;
pub mod grpc;

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();
    bootstrap::run().await
}
