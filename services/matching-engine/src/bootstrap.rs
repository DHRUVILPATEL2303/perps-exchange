use crate::infrastructure::kafka::consumer::OrderConsumer;
use crate::infrastructure::kafka::producer::TradeProducer;
use anyhow::Result;
use config::app::AppConfig;
use std::sync::Arc;

pub async fn bootstrap() -> Result<()> {
    let config = AppConfig::load("matching-engine").expect("Failed to load config");
    telemetry::http::spawn_metrics_server(config.server.port);


    let brokers = config.kafka.brokers.join(",");
    
    let redis_url = format!("redis://{}:{}", config.redis.host, config.redis.port);
    let redis_client = redis::Client::open(redis_url)?;
    let redis_conn = redis_client.get_multiplexed_async_connection().await?;

    let producer = Arc::new(TradeProducer::new(&brokers, redis_conn)?);

    let consumer = OrderConsumer::new(&brokers, "matching-engine-group", producer)?;

    tracing::info!("Matching Engine started. Consuming from order-events topic...");

    consumer.run().await;

    Ok(())
}
