use config::app::AppConfig;
use database::manager::DatabaseManager;
use std::sync::Arc;

use crate::application::usecase::market_usecase::MarketUseCase;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub db: Arc<DatabaseManager>,
    pub market_service: Arc<dyn MarketUseCase>,
}
