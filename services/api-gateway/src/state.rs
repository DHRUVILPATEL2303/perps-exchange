use std::sync::Arc;

use config::settings::AppConfig;


#[derive(Clone)]
pub struct AppState {
    pub config : Arc<AppConfig>
}
