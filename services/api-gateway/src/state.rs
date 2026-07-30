use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use config::app::AppConfig;
use proto::market::market_service_client::MarketServiceClient;
use proto::account::account_service_client::AccountServiceClient;
use proto::trading::trading_service_client::TradingServiceClient;


#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub market_client: MarketServiceClient<Channel>,
    pub account_client: AccountServiceClient<Channel>,
    pub trading_client: TradingServiceClient<Channel>,
}

