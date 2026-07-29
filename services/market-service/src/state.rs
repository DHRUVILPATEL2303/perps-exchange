use config::app::AppConfig;
use database::manager::DatabaseManager;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub db: Arc<DatabaseManager>,
}
