use anyhow::Result;
use proto::account::{AdjustMarginRequest, account_service_client::AccountServiceClient};
use proto::trading::{PlaceOrderRequest, trading_service_client::TradingServiceClient};
use rand::Rng;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Perp Exchange Load Tester ===");

    let concurrency = std::env::var("CONCURRENCY")
        .unwrap_or_else(|_| "20".to_string())
        .parse::<usize>()?;
    let duration_secs = std::env::var("DURATION_SECS")
        .unwrap_or_else(|_| "15".to_string())
        .parse::<u64>()?;
    let user_pool_size = std::env::var("USER_POOL")
        .unwrap_or_else(|_| "500".to_string())
        .parse::<usize>()?;
    let max_orders = std::env::var("MAX_ORDERS")
        .ok()
        .and_then(|val| val.parse::<usize>().ok());

    let account_url = "http://127.0.0.1:50053";
    let trading_url = "http://127.0.0.1:50052";

    println!("Target Concurrency: {}", concurrency);
    println!("User Pool Size: {}", user_pool_size);
    if let Some(limit) = max_orders {
        println!("Target Order Count: {}", limit);
    } else {
        println!("Duration: {} seconds", duration_secs);
    }

    println!("Generating user pool...");
    let mut users = Vec::with_capacity(user_pool_size);
    for _ in 0..user_pool_size {
        users.push(Uuid::new_v4());
    }

    println!("Connecting to Account Service: {}...", account_url);
    let mut account_client = AccountServiceClient::connect(account_url).await?;

    println!("Depositing funds (1,000,000 USDT) into each user account...");
    let start_deposit = Instant::now();
    for (i, user) in users.iter().enumerate() {
        let req = AdjustMarginRequest {
            user_id: user.to_string(),
            amount: "1000000.00".to_string(),
            adjustment_type: "DEPOSIT".to_string(),
            ..Default::default()
        };
        account_client.adjust_margin(req).await?;
        if (i + 1) % 100 == 0 || i + 1 == user_pool_size {
            println!("  Deposited {}/{} users...", i + 1, user_pool_size);
        }
    }
    println!("Deposits completed in {:?}", start_deposit.elapsed());

    println!(
        "Preparing trading load on trading-service: {}...",
        trading_url
    );
    let users = Arc::new(users);
    let total_sent = Arc::new(AtomicUsize::new(0));
    let total_success = Arc::new(Mutex::new(0));
    let total_failed = Arc::new(Mutex::new(0));
    let latencies = Arc::new(Mutex::new(Vec::new()));

    let start_load = Instant::now();
    let end_time = start_load + Duration::from_secs(duration_secs);

    let mut handles = Vec::new();

    for worker_id in 0..concurrency {
        let users = users.clone();
        let total_sent = total_sent.clone();
        let total_success = total_success.clone();
        let total_failed = total_failed.clone();
        let latencies = latencies.clone();

        handles.push(tokio::spawn(async move {
            let mut trading_client = match TradingServiceClient::connect(trading_url).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Worker {} failed to connect: {:?}", worker_id, e);
                    return;
                }
            };

            while Instant::now() < end_time {
                if let Some(limit) = max_orders {
                    if total_sent.load(Ordering::Relaxed) >= limit {
                        break;
                    }
                }

                let (user_id, side, price) = {
                    let mut rng = rand::thread_rng();
                    let user_idx = rng.gen_range(0..users.len());
                    let user_id = users[user_idx];
                    let side = if rng.gen_bool(0.5) { "BUY" } else { "SELL" };
                    let price = if side == "BUY" {
                        rng.gen_range(62505..62520).to_string()
                    } else {
                        rng.gen_range(62480..62495).to_string()
                    };
                    (user_id, side, price)
                };

                let request = PlaceOrderRequest {
                    user_id: user_id.to_string(),
                    symbol: "BTCUSDT".to_string(),
                    side: side.to_string(),
                    order_type: "LIMIT".to_string(),
                    quantity: "0.05".to_string(),
                    price: Some(price),
                    trigger_price: None,
                    time_in_force: "GTC".to_string(),
                    leverage: 20,
                    margin_mode: "ISOLATED".to_string(),
                    reduce_only: false,
                    post_only: false,
                };

                let request_start = Instant::now();
                let res = trading_client.place_order(request).await;
                let duration = request_start.elapsed().as_secs_f64();

                total_sent.fetch_add(1, Ordering::Relaxed);

                match res {
                    Ok(resp) => {
                        let response_inner = resp.into_inner();
                        if response_inner.status == "OPEN" {
                            let mut succ = total_success.lock().await;
                            *succ += 1;
                        } else {
                            let mut fail = total_failed.lock().await;
                            *fail += 1;
                        }
                    }
                    Err(_) => {
                        let mut fail = total_failed.lock().await;
                        *fail += 1;
                    }
                }

                {
                    let mut lat = latencies.lock().await;
                    lat.push(duration);
                }

                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let total_duration = start_load.elapsed();

    let sent = total_sent.load(Ordering::Relaxed);
    let success = *total_success.lock().await;
    let failed = *total_failed.lock().await;
    let lat_vec = latencies.lock().await;

    let ops = sent as f64 / total_duration.as_secs_f64();

    println!("\n=== Client Load Test Summary ===");
    println!("Total Duration: {:?}", total_duration);
    println!("Total Orders Sent: {}", sent);
    println!("Successful Orders (OPEN): {}", success);
    println!("Failed/Rejected Orders: {}", failed);
    println!("Throughput: {:.2} orders/sec", ops);

    if !lat_vec.is_empty() {
        let sum: f64 = lat_vec.iter().sum();
        let avg = sum / lat_vec.len() as f64;
        let min = lat_vec.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max = lat_vec.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        println!("Average Latency (Client-side gRPC): {:.2} ms", avg * 1000.0);
        println!("Min Latency: {:.2} ms", min * 1000.0);
        println!("Max Latency: {:.2} ms", max * 1000.0);
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("\n=== Backend Server-Side Performance Metrics ===");
    fetch_and_print_metrics("Trading Service", "http://127.0.0.1:8082/metrics").await;
    fetch_and_print_metrics("Matching Engine", "http://127.0.0.1:8086/metrics").await;

    Ok(())
}

async fn fetch_and_print_metrics(service_name: &str, url: &str) {
    println!("\nMetrics for {}:", service_name);
    match reqwest::get(url).await {
        Ok(res) => {
            if let Ok(body) = res.text().await {
                parse_and_display_metric(
                    &body,
                    "trading_risk_check_duration_seconds",
                    "Risk Engine Check",
                );
                parse_and_display_metric(
                    &body,
                    "trading_db_insert_duration_seconds",
                    "Postgres Order Insert",
                );
                parse_and_display_metric(
                    &body,
                    "trading_margin_lock_duration_seconds",
                    "Account Margin Lock",
                );
                parse_and_display_metric(
                    &body,
                    "trading_kafka_publish_duration_seconds",
                    "Kafka Order Publish",
                );
                parse_and_display_metric(
                    &body,
                    "order_transit_duration_seconds",
                    "Kafka Transit Queue Latency",
                );
                parse_and_display_metric(
                    &body,
                    "matching_duration_seconds",
                    "Orderbook Match Execution",
                );
                parse_and_display_metric(
                    &body,
                    "grpc_request_duration_seconds",
                    "Overall gRPC Handler",
                );
            }
        }
        Err(e) => {
            eprintln!("  Failed to fetch metrics from {}: {:?}", url, e);
        }
    }
}

fn parse_and_display_metric(body: &str, metric_name: &str, display_name: &str) {
    let sum_prefix = format!("{}_sum", metric_name);
    let count_prefix = format!("{}_count", metric_name);

    let mut sum: Option<f64> = None;
    let mut count: Option<f64> = None;

    for line in body.lines() {
        if line.starts_with(&sum_prefix) {
            if let Some(val_str) = line.split_whitespace().last() {
                sum = val_str.parse().ok();
            }
        } else if line.starts_with(&count_prefix) {
            if let Some(val_str) = line.split_whitespace().last() {
                count = val_str.parse().ok();
            }
        }
    }

    if let (Some(s), Some(c)) = (sum, count) {
        if c > 0.0 {
            let avg_ms = (s / c) * 1000.0;
            println!(
                "  - {}: Average = {:.2} ms (Count = {})",
                display_name, avg_ms, c
            );
        }
    }
}
