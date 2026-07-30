use std::sync::Arc;
use config::app::AppConfig;
use database::manager::DatabaseManager;
use crate::application::usecase::account_usecase::AccountUseCase;

pub struct AppState {
    pub config: Arc<AppConfig>,
    pub db: Arc<DatabaseManager>,
    pub account_service: Arc<dyn AccountUseCase>,
}
