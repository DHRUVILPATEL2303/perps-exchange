use anyhow::Result;

pub mod application;
pub mod bootstrap;
pub mod domain;
pub mod infrastructure;
pub mod state;

#[tokio::main]
async fn main() -> Result<()> {
    let state = bootstrap::bootstrap().await?;

    println!(
        "Trading Service Ready. Markets: {}",
        state.market_cache.len().await
    );

    Ok(())
}
