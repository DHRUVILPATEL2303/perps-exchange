use config::app::AppConfig;
use proto::account::account_service_client::AccountServiceClient;
use proto::chart::chart_service_client::ChartServiceClient;
use proto::market::market_service_client::MarketServiceClient;
use proto::trading::trading_service_client::TradingServiceClient;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::Mutex;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub market_client: MarketServiceClient<Channel>,
    pub account_client: AccountServiceClient<Channel>,
    pub trading_clients: Vec<TradingServiceClient<Channel>>,
    pub trading_pool_index: Arc<AtomicUsize>,
    pub ws_sessions: Arc<Mutex<Vec<actix_ws::Session>>>,
    pub redis_client: redis::Client,
    pub chart_client: ChartServiceClient<Channel>,
}
