use anyhow::Result;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod application;
pub mod bootstrap;
pub mod domain;
pub mod infrastructure;

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();
    bootstrap::bootstrap().await
}
