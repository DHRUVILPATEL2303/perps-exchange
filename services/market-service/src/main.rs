use std::io::Result;

pub mod state;
pub mod bootstrap;
pub use sqlx;
pub mod domain;


#[tokio::main]
async fn main() -> Result<()>{
    telemetry::logging::init();
    bootstrap::run().await
}
