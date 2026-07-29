use config::{Config, ConfigError, Environment, File};

use crate::app::AppConfig;

impl AppConfig {
    pub fn load(service_name: &str) -> Result<Self, ConfigError> {
        dotenvy::dotenv().ok();

        Config::builder()
            //shared config
            .add_source(File::with_name("configs/common"))
            // specific service configuration
            .add_source(File::with_name(&format!(
                "configs/{}",
                service_name
            )))
            .add_source(Environment::default().separator("__"))
            .build()?
            .try_deserialize()
    }
}