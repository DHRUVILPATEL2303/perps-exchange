use anyhow::Result;


pub mod state;
pub mod bootstrap;
pub mod infrastructure;
pub mod domain;
pub mod application;


#[tokio::main]
async fn main() -> Result<()> {

    let state = bootstrap::bootstrap().await?;

    println!(
        "Trading Service Ready. Markets: {}",
        state.market_cache.len().await
    );

    Ok(())
}
