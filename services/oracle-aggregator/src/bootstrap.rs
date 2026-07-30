use std::sync::Arc;
use anyhow::Result;
use config::app::AppConfig;
use crate::application::services::aggregator_service::AggregatorService;
use crate::infrastructure::kafka::producer::KafkaPricePublisher;
use crate::state::AppState;

pub async fn run() -> Result<()> {
    let config = AppConfig::load("oracle-aggregator").expect("Failed to load config");
    
    let kafka_brokers = config.kafka.brokers.join(",");
    let publisher = Arc::new(KafkaPricePublisher::new(
        &kafka_brokers,
        "price-feed".to_string(),
    ));

    let aggregator_service = Arc::new(AggregatorService::new(publisher));

    let _state = AppState {
        config: Arc::new(config),
        aggregator_service: aggregator_service.clone(),
    };

    aggregator_service.start().await?;

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}
