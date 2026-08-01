use anyhow::Result;
use futures_util::StreamExt;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::str::FromStr;

#[derive(Deserialize, Debug, Clone)]
struct KafkaTradeEvent {
    symbol: String,
    price: String,
    quantity: String,
    taker_side: String,
    maker_user_id: String,
    taker_user_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Candle {
    pub timestamp: i64,
    pub open: rust_decimal::Decimal,
    pub high: rust_decimal::Decimal,
    pub low: rust_decimal::Decimal,
    pub close: rust_decimal::Decimal,
    pub volume: rust_decimal::Decimal,
}

async fn save_trade_to_db(db_pool: &PgPool, trade: &KafkaTradeEvent) -> Result<()> {
    let now = chrono::Utc::now();
    let price = rust_decimal::Decimal::from_str(&trade.price)?;
    let quantity = rust_decimal::Decimal::from_str(&trade.quantity)?;

    let query = r#"
        INSERT INTO trades (time, symbol, price, quantity, taker_side)
        VALUES ($1, $2, $3, $4, $5)
    "#;

    sqlx::query(query)
        .bind(now)
        .bind(&trade.symbol)
        .bind(price)
        .bind(quantity)
        .bind(&trade.taker_side)
        .execute(db_pool)
        .await?;

    Ok(())
}

async fn update_redis_candle(
    redis_conn: &mut redis::aio::MultiplexedConnection,
    trade: &KafkaTradeEvent,
    resolution_secs: i64,
    resolution_label: &str,
) -> Result<()> {
    let trade_time = chrono::Utc::now().timestamp();
    let bucket_time = trade_time - (trade_time % resolution_secs);
    let key = format!("candles:{}:{}", trade.symbol, resolution_label);

    let price = rust_decimal::Decimal::from_str(&trade.price)?;
    let quantity = rust_decimal::Decimal::from_str(&trade.quantity)?;

    let existing_json: Vec<String> = redis_conn
        .zrangebyscore_limit(&key, bucket_time, bucket_time, 0, 1)
        .await?;

    let candle = match existing_json.first() {
        Some(json_str) => {
            let mut c: Candle = serde_json::from_str(json_str)?;
            c.high = c.high.max(price);
            c.low = c.low.min(price);
            c.close = price;
            c.volume += quantity;
            c
        }
        None => Candle {
            timestamp: bucket_time,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: quantity,
        },
    };

    let candle_json = serde_json::to_string(&candle)?;

    let _: () = redis_conn
        .zrembyscore(&key, bucket_time, bucket_time)
        .await?;
    let _: () = redis_conn.zadd(&key, &candle_json, bucket_time).await?;
    let _: () = redis_conn.zremrangebyrank(&key, 0, -1001).await?;

    let channel = format!("candles:{}:{}", trade.symbol, resolution_label);
    let _: () = redis_conn.publish(channel, candle_json).await?;

    Ok(())
}

pub async fn run_trade_consumer(
    brokers: &str,
    db_pool: PgPool,
    redis_client: redis::Client,
) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "chart-service-group")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .create()
        .expect("Failed to create Kafka consumer for charts");

    consumer
        .subscribe(&["execution-reports"])
        .expect("Failed to subscribe to execution-reports topic");

    println!("Chart Service consumer running. Connected to TimescaleDB and Redis.");

    let mut stream = consumer.stream();
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;

    while let Some(msg_result) = stream.next().await {
        match msg_result {
            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    match serde_json::from_slice::<KafkaTradeEvent>(payload) {
                        Ok(trade) => {
                            println!("Ingested trade event from Kafka: {:?}", trade);

                            if let Err(e) = save_trade_to_db(&db_pool, &trade).await {
                                eprintln!("TimescaleDB save failed: {:?}", e);
                            }

                            if let Err(e) = update_redis_candle(&mut redis_conn, &trade, 60, "1m").await
                            {
                                eprintln!("Redis 1m update failed: {:?}", e);
                            }
                            if let Err(e) =
                                update_redis_candle(&mut redis_conn, &trade, 300, "5m").await
                            {
                                eprintln!("Redis 5m update failed: {:?}", e);
                            }
                            if let Err(e) =
                                update_redis_candle(&mut redis_conn, &trade, 3600, "1h").await
                            {
                                eprintln!("Redis 1h update failed: {:?}", e);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to deserialize KafkaTradeEvent payload: {:?}. Raw: {:?}", e, String::from_utf8_lossy(payload));
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
