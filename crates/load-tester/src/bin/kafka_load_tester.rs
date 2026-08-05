use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use uuid::Uuid;

use rust_decimal::Decimal;
use std::str::FromStr;

#[derive(Serialize)]
pub struct KafkaOrderEvent {
    pub id: Uuid,
    pub user_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub price: String,
    pub quantity: String,
    pub action: String,
    pub timestamp: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let brokers = "localhost:9092";

    println!("=== Matching Engine Direct Kafka Load Tester ===");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .set("queue.buffering.max.messages", "1000000")
        .set("batch.num.messages", "10000")
        .set("linger.ms", "5")
        .create()?;

    let target_orders = 50_000_000;
    println!(
        "Pushing {} orders directly to Kafka topic 'order-events'...",
        target_orders
    );

    let start_time = Instant::now();
    let sent_count = Arc::new(AtomicUsize::new(0));

    let price_buy = Decimal::from_str("60000.00").unwrap();
    let price_sell = Decimal::from_str("60010.00").unwrap();
    let qty = Decimal::from_str("0.1").unwrap();

    for i in 0..target_orders {
        let id = Uuid::new_v4();
        let order = KafkaOrderEvent {
            id,
            user_id: Uuid::new_v4(),
            symbol: "BTCUSDT".to_string(),
            side: if i % 2 == 0 {
                "BUY".to_string()
            } else {
                "SELL".to_string()
            },
            order_type: "LIMIT".to_string(),
            price: if i % 2 == 0 {
                price_buy.to_string()
            } else {
                price_sell.to_string()
            },
            quantity: qty.to_string(),
            action: "PLACE".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
        };

        let payload = bincode::serialize(&order)?;
        let key = id.as_bytes();

        let _ = producer.send_result(
            FutureRecord::to("order-events")
                .payload(payload.as_slice())
                .key(key),
        );

        sent_count.fetch_add(1, Ordering::Relaxed);

        if (i + 1) % 200_000 == 0 {
            println!("Pushed {} orders...", i + 1);
        }
    }

    println!("Flushing producer (waiting for acks)...");
    producer.flush(Duration::from_secs(10));

    let elapsed = start_time.elapsed();
    let ops = target_orders as f64 / elapsed.as_secs_f64();

    println!("Finished! Pushed {} orders in {:?}", target_orders, elapsed);
    println!("Throughput: {:.2} messages/sec to Kafka", ops);

    Ok(())
}
