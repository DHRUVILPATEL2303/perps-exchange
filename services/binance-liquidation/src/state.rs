use std::sync::Arc;
use config::app::AppConfig;
use crate::application::services::liquidation_service::ListenerService;

pub struct AppState {
    pub config: Arc<AppConfig>,
    pub listener_service: Arc<ListenerService>,
}
