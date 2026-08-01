use anyhow::Result;
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use serde::Deserialize;
use sqlx::PgPool;

#[derive(Deserialize, Debug)]
struct KafkaTradeEvent {
    symbol: String,
    price: rust_decimal::Decimal,
    quantity: rust_decimal::Decimal,
    taker_side: String,
    maker_user_id: String,
    taker_user_id: String,
}

async fn save_trade_to_db(db_pool: &PgPool, trade: &KafkaTradeEvent) -> Result<()> {
    let now = chrono::Utc::now();
    let query = r#"
        INSERT INTO trades (time, symbol, price, quantity, taker_side)
        VALUES ($1, $2, $3, $4, $5)
    "#;

    sqlx::query(query)
        .bind(now)
        .bind(&trade.symbol)
        .bind(trade.price)
        .bind(trade.quantity)
        .bind(&trade.taker_side)
        .execute(db_pool)
        .await?;

    println!(
        "Logged trade to TimescaleDB: {} at {} (Price: {}, Quantity: {})",
        trade.symbol, now, trade.price, trade.quantity
    );
    Ok(())
}

pub async fn run_trade_consumer(brokers: &str, db_pool: PgPool) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "chart-service-group")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .create()
        .expect("Failed to create Kafka consumer for charts");

    consumer.subscribe(&["execution-reports"])
        .expect("Failed to subscribe to execution-reports topic");

    println!("Chart Service consumer subscribed to execution-reports");

    let mut stream = consumer.stream();

    while let Some(msg_result) = stream.next().await {
        match msg_result {
            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    if let Ok(trade) = serde_json::from_slice::<KafkaTradeEvent>(payload) {
                        if let Err(e) = save_trade_to_db(&db_pool, &trade).await {
                            eprintln!("Failed to save trade to database: {:?}", e);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Kafka message consumption error: {:?}", e);
            }
        }
    }

    Ok(())
}
