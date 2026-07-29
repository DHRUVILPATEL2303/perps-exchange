use std::io::Result;

pub mod state;
pub mod bootstrap;
pub use sqlx;
pub mod domain;
pub mod infrastructure;
pub mod application;
pub mod presentation;

#[tokio::main]
async fn main() -> Result<()>{
    telemetry::logging::init();
    bootstrap::run().await
}
