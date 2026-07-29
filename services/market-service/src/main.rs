use std::io::Result;

pub mod state;
pub mod bootstrap;


#[tokio::main]
async fn main() -> Result<()>{
    telemetry::logging::init();
    bootstrap::run().await
}
