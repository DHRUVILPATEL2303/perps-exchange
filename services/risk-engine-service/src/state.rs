use std::sync::Arc;
use config::app::AppConfig;
use database::manager::DatabaseManager;
use crate::infrastructure::grpc::{account_client::AccountGrpcClient, trading_client::TradingGrpcClient};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub db: Arc<DatabaseManager>,
    pub account_client: AccountGrpcClient,
    pub trading_client: TradingGrpcClient,
}
