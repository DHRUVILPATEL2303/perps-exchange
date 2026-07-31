use crate::application::services::aggregator_service::AggregatorService;
use crate::infrastructure::kafka::producer::KafkaPricePublisher;
use crate::state::AppState;
use anyhow::Result;
use config::app::AppConfig;
use std::sync::Arc;

pub async fn run() -> Result<()> {
    let config = AppConfig::load("oracle-aggregator").expect("Failed to load config");

    let brokers = config.kafka.brokers.join(",");
    let publisher = Arc::new(KafkaPricePublisher::new(&brokers, "price-feed".to_string()));
    let redis_url = format!("redis://{}:{}", config.redis.host, config.redis.port);
    let redis_client = redis::Client::open(redis_url).expect("Failed to open Redis client");

    let aggregator_service = Arc::new(AggregatorService::new(publisher, redis_client));
    let _state = AppState {
        config: Arc::new(config),
        aggregator_service: aggregator_service.clone(),
    };

    aggregator_service.start().await?;

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}
