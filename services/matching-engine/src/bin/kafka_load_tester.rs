use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use serde::Serialize;
use std::time::Instant;
use uuid::Uuid;

#[derive(Serialize)]
pub struct IncomingOrder {
    pub id: Uuid,
    pub user_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub price: String,
    pub quantity: String,
    pub action: String,
    pub timestamp: u64,
    pub leverage: u32,
    pub reduce_only: bool,
    pub post_only: bool,
}

fn main() {
    let brokers = "localhost:9092";
    let topic = "order-events";
    let num_orders = 200_000_00;

    println!("Building Kafka Producer...");
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("queue.buffering.max.messages", "500000")
        .set("queue.buffering.max.kbytes", "1048576")
        .set("batch.num.messages", "10000")
        .set("linger.ms", "5")
        .create()
        .expect("Failed to create Kafka producer");

    println!(
        "Firing {} orders directly into '{}' topic...",
        num_orders, topic
    );

    let start_time = Instant::now();

    for i in 0..num_orders {
        let order = IncomingOrder {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            symbol: "BTCUSDT".to_string(),
            side: if i % 2 == 0 {
                "BUY".to_string()
            } else {
                "SELL".to_string()
            },
            order_type: "LIMIT".to_string(),
            price: if i % 2 == 0 {
                "60000.00".to_string()
            } else {
                "59000.00".to_string()
            },
            quantity: "0.1".to_string(),
            action: "PLACE".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            leverage: 1,
            reduce_only: false,
            post_only: false,
        };

        let payload = bincode::serialize(&order).unwrap();

        let mut record = BaseRecord::to(topic)
            .payload(payload.as_slice())
            .key(b"BTCUSDT");

        loop {
            match producer.send(record) {
                Ok(_) => break,
                Err((e, returned_record)) => {
                    if e == rdkafka::error::KafkaError::MessageProduction(
                        rdkafka::types::RDKafkaErrorCode::QueueFull,
                    ) {
                        producer.poll(std::time::Duration::from_millis(10));
                        record = returned_record;
                    } else {
                        panic!("Kafka error: {:?}", e);
                    }
                }
            }
        }

        if i > 0 && i % 50_000 == 0 {
            println!("Enqueued {} orders...", i);
        }
    }

    println!("Flushing network buffers...");
    producer.flush(std::time::Duration::from_secs(10)).unwrap();

    let duration = start_time.elapsed().as_secs_f64();
    println!("✓ Complete!");
    println!("Total time : {:.2}s", duration);
    println!(
        "Throughput : {:.0} orders/sec",
        (num_orders as f64) / duration
    );
}
