use std::sync::Arc;

use anyhow::Result;

use crate::{
    application::services::trading_service::TradingService,
    infrastructure::{
        cache::market_cache::MarketCache,
        grpc::market_client::MarketGrpcClient,
    },
    state::AppState,
};

pub async fn bootstrap() -> Result<AppState> {

    println!("Connecting to Market Service...");

    let mut client =
        MarketGrpcClient::connect(
            "http://127.0.0.1:50051".into(),
        )
        .await?;

    println!("Loading markets...");

    let markets = client.list_markets().await?;

    println!("Loaded {} markets", markets.len());

    let market_cache = Arc::new(MarketCache::new());

    market_cache.load(markets).await;

    println!(
        "Market cache initialized with {} markets",
        market_cache.len().await
    );

    let trading_service =
        Arc::new(
            TradingService::new(
                market_cache.clone(),
            ),
        );

    Ok(AppState {
        market_cache,
        trading_service,
    })
}