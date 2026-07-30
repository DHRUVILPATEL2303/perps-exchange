use std::sync::Arc;

use config::app::AppConfig;

use crate::application::services::health_service::HealthService;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub health_service: Arc<HealthService>,
}
