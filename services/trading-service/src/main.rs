use anyhow::Result;


use anyhow::Ok;

use crate::infrastructure::grpc::market_client::MarketGrpcClient;

pub mod infrastructure;
pub mod domain;

#[tokio::main]
async  fn main() -> Result<()> {
    let mut market_client =
        MarketGrpcClient::connect("http://127.0.0.1:50051".to_string())
            .await
            .expect("Failed to connect to Market Service");

    
    let markets = market_client.list_markets().await?;
    
    println!("Loaded {} markets", markets.len());
    
    for market in &markets {
        println!("{:?}", market);
    }

    Ok(())
}
