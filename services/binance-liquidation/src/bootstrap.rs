use std::sync::Arc;
use anyhow::Result;
use config::app::AppConfig;
use crate::application::services::liquidation_service::ListenerService;
use crate::infrastructure::kafka::producer::KafkaLiquidationPublisher;
use crate::state::AppState;

pub async fn run() -> Result<()> {
    let config = AppConfig::load("binance-liquidation").expect("Failed to load config");
    
    let kafka_brokers = config.kafka.brokers.join(",");
    let publisher = Arc::new(KafkaLiquidationPublisher::new(
        &kafka_brokers,
        "binance-liquidations".to_string(),
    ));

    let ws_url = config.websocket.as_ref()
        .map(|w| w.url.clone())
        .unwrap_or_else(|| "wss://fstream.binance.com/ws/!forceOrder@arr".to_string());

    let listener_service = Arc::new(ListenerService::new(publisher, ws_url));

    let state = AppState {
        config: Arc::new(config),
        listener_service: listener_service.clone(),
    };

    listener_service.start().await?;

    Ok(())
}
