use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// ── Config ─────────────────────────────────────────────────────────────
const BROKERS: &str = "localhost:9092";
const TOPIC: &str = "order-events";
const SYMBOL: &str = "BTCUSDT";

/// How many BUY+SELL pairs to send per batch
const BATCH_SIZE: usize = 1_000;

/// Delay between batches (ms). Adjust to control pace:
///   0  = max throughput (stress test)
///   10 = ~200k orders/s paced
///   100 = ~20k orders/s — gentle, good for watching order book live
const BATCH_DELAY_MS: u64 = 50;

/// Total orders to send (BUY + SELL pairs, so actual sent = TOTAL_ORDERS)
const TOTAL_ORDERS: usize = 200_000;

/// Mid-market price. BUYs are sent AT or ABOVE this, SELLs AT or BELOW.
/// This guarantees they cross and the matching engine executes them.
///
/// Spread ladder: we send orders at PRICE ± spread_offset so the book
/// has depth but orders still match.
const MID_PRICE: f64 = 60_000.0;

/// Half-spread: BUY prices range [MID - HALF_SPREAD, MID + 1],
///              SELL prices range [MID - 1, MID + HALF_SPREAD].
/// Because BUY >= SELL in any pair, they cross and match immediately.
const HALF_SPREAD: f64 = 20.0;

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
    pub leverage: u32,
    pub reduce_only: bool,
    pub post_only: bool,
}

fn micros_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

/// Returns a (buy_price, sell_price) pair that always crosses:
///   buy_price  >= sell_price  => immediate match
fn crossing_pair(i: usize) -> (f64, f64) {
    // Vary prices across the spread so the book has realistic depth
    let offset = (i % 20) as f64 * (HALF_SPREAD / 20.0);
    let buy_price  = MID_PRICE + offset;         // e.g. 60000 → 60020
    let sell_price = MID_PRICE - offset;         // e.g. 60000 → 59980
    // sell <= mid <= buy  ⟹  buy crosses sell ✅
    (buy_price, sell_price)
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("══════════════════════════════════════════════════════");
    println!("  dpkv perps — Matching Engine Kafka Load Tester");
    println!("══════════════════════════════════════════════════════");
    println!("  Broker      : {}", BROKERS);
    println!("  Topic       : {}", TOPIC);
    println!("  Symbol      : {}", SYMBOL);
    println!("  Orders      : {} (BUY+SELL pairs, all crossing)", TOTAL_ORDERS);
    println!("  Batch size  : {} orders", BATCH_SIZE * 2);
    println!("  Batch delay : {} ms", BATCH_DELAY_MS);
    println!("  Mid price   : ${:.2}  half-spread: ${:.2}", MID_PRICE, HALF_SPREAD);
    println!("══════════════════════════════════════════════════════\n");

    let producer: Arc<FutureProducer> = Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", BROKERS)
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.messages", "1000000")
            .set("queue.buffering.max.kbytes", "512000")
            .set("batch.num.messages", "10000")
            .set("linger.ms", "5")
            .set("compression.type", "lz4")
            .create()?,
    );

    let sent = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let batches = TOTAL_ORDERS / BATCH_SIZE;

    for batch in 0..batches {
        let base_i = batch * BATCH_SIZE;
        let mut messages: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BATCH_SIZE * 2);

        for j in 0..BATCH_SIZE {
            let i = base_i + j;
            let (buy_price, sell_price) = crossing_pair(i);

            // BUY order
            let buy_id = Uuid::new_v4();
            let buy = KafkaOrderEvent {
                id: buy_id,
                user_id: Uuid::new_v4(),
                symbol: SYMBOL.to_string(),
                side: "BUY".to_string(),
                order_type: "LIMIT".to_string(),
                price: format!("{:.2}", buy_price),
                quantity: "0.10".to_string(),
                action: "PLACE".to_string(),
                timestamp: micros_now(),
                leverage: 1,
                reduce_only: false,
                post_only: false,
            };

            // SELL order — same quantity, lower price → crosses with BUY above
            let sell_id = Uuid::new_v4();
            let sell = KafkaOrderEvent {
                id: sell_id,
                user_id: Uuid::new_v4(),
                symbol: SYMBOL.to_string(),
                side: "SELL".to_string(),
                order_type: "LIMIT".to_string(),
                price: format!("{:.2}", sell_price),
                quantity: "0.10".to_string(),
                action: "PLACE".to_string(),
                timestamp: micros_now(),
                leverage: 1,
                reduce_only: false,
                post_only: false,
            };

            messages.push((buy_id.as_bytes().to_vec(),  bincode::serialize(&buy)?));
            messages.push((sell_id.as_bytes().to_vec(), bincode::serialize(&sell)?));
        }

        // Fire all messages in this batch (non-blocking)
        for (key, payload) in &messages {
            let _ = producer.send_result(
                FutureRecord::to(TOPIC)
                    .payload(payload.as_slice())
                    .key(key.as_slice()),
            );
        }

        sent.fetch_add(messages.len(), Ordering::Relaxed);

        let total_sent = sent.load(Ordering::Relaxed);
        let elapsed    = start.elapsed().as_secs_f64();
        let rate       = total_sent as f64 / elapsed;

        print!(
            "\r  [Batch {:>4}/{:<4}]  Sent: {:>8}  |  Rate: {:>9.0} msg/s  |  {:.1}s ",
            batch + 1, batches, total_sent, rate, elapsed
        );

        if BATCH_DELAY_MS > 0 {
            tokio::time::sleep(Duration::from_millis(BATCH_DELAY_MS)).await;
        }
    }

    println!("\n\n  Flushing producer (waiting for broker acks)...");
    let _ = producer.flush(Duration::from_secs(15));

    let elapsed = start.elapsed();
    let total   = sent.load(Ordering::Relaxed);
    let ops     = total as f64 / elapsed.as_secs_f64();

    println!("\n══════════════════════════════════════════════════════");
    println!("  RESULTS");
    println!("══════════════════════════════════════════════════════");
    println!("  Total sent  : {} messages", total);
    println!("  Duration    : {:.2}s", elapsed.as_secs_f64());
    println!("  Throughput  : {:.0} msg/s to Kafka", ops);
    println!("  Expected matches: ~{} trades", total / 2);
    println!("══════════════════════════════════════════════════════\n");

    Ok(())
}

