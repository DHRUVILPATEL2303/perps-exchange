use std::sync::Arc;
use config::app::AppConfig;
use crate::application::services::aggregator_service::AggregatorService;

pub struct AppState {
    pub config: Arc<AppConfig>,
    pub aggregator_service: Arc<AggregatorService>,
}
