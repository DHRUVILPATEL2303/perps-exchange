use std::sync::Arc;
use config::app::AppConfig;
use database::manager::DatabaseManager;
use crate::{
    application::services::trading_service::TradingService,
    application::usecase::position_usecase::PositionUseCase,
    domain::repositories::order_repository::OrderRepository,
    infrastructure::{
        cache::market_cache::MarketCache,
        grpc::risk_client::RiskGrpcClient,
    },
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub db: Arc<DatabaseManager>,
    pub trading_service: Arc<TradingService>,
    pub market_cache: Arc<MarketCache>,
    pub position_service: Arc<dyn PositionUseCase>,
    pub risk_client: Arc<RiskGrpcClient>,
    pub order_publisher: Arc<aeron_transport::AeronPublisher>,
    pub order_repository: Arc<dyn OrderRepository>,
}
