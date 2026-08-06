use std::sync::Arc;
use tokio::sync::Mutex;
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;
use rust_decimal::Decimal;
use redis::AsyncCommands;
use futures_util::StreamExt;
use crate::{
    domain::price_tracker::PriceTracker,
    infrastructure::{
        grpc::{account_client::AccountGrpcClient, risk_client::RiskGrpcClient},
        kafka::producer::{KafkaOrderEvent, OrderProducer},
    },
};

#[derive(serde::Deserialize, Debug)]
struct PriceTick {
    pub symbol: String,
    pub mark_price: Decimal,
}

pub fn start_trigger_loop(
    db_pool: Pool<Postgres>,
    redis_url: String,
    price_tracker: PriceTracker,
    account_client: AccountGrpcClient,
    risk_client: RiskGrpcClient,
    order_producer: Arc<OrderProducer>,
) {
    tokio::spawn(async move {
        tracing::info!("Starting conditional order trigger loop subscribing to Redis price-ticks...");
        
        let client = match redis::Client::open(redis_url) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("Failed to open Redis client for trigger loop: {:?}", e);
                return;
            }
        };

        let mut conn = match client.get_async_pubsub().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!("Failed to get Redis pubsub connection: {:?}", e);
                return;
            }
        };

        if let Err(e) = conn.subscribe("price-ticks").await {
            tracing::error!("Failed to subscribe to price-ticks channel: {:?}", e);
            return;
        }

        let mut stream = conn.on_message();

        while let Some(msg) = stream.next().await {
            let payload: String = match msg.get_payload() {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!("Failed to get message payload: {:?}", e);
                    continue;
                }
            };

            let tick: PriceTick = match serde_json::from_str(&payload) {
                Ok(t) => t,
                Err(e) => {
                    tracing::error!("Failed to deserialize price tick: {:?}, payload: {}", e, payload);
                    continue;
                }
            };

            price_tracker.set_price(tick.symbol.clone(), tick.mark_price).await;

            let triggered_orders = match sqlx::query(
                r#"
                UPDATE orders
                SET status = 'OPEN', updated_at = NOW()
                WHERE id IN (
                    SELECT id FROM orders
                    WHERE symbol = $1
                      AND status = 'PENDING_TRIGGER'
                      AND (
                          (trigger_direction = 'ABOVE' AND $2 >= trigger_price)
                          OR
                          (trigger_direction = 'BELOW' AND $2 <= trigger_price)
                      )
                )
                RETURNING id, user_id, symbol, side, order_type, price, quantity, status, leverage, reduce_only, post_only
                "#,
            )
            .bind(&tick.symbol)
            .bind(tick.mark_price)
            .fetch_all(&db_pool)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::error!("Failed to query and update triggered orders: {:?}", e);
                    continue;
                }
            };

            for row in triggered_orders {
                let order_id: Uuid = row.get("id");
                let user_id: Uuid = row.get("user_id");
                let symbol: String = row.get("symbol");
                let side: String = row.get("side");
                let order_type: String = row.get("order_type");
                let price: Decimal = row.get("price");
                let quantity: Decimal = row.get("quantity");
                let leverage: i32 = row.get("leverage");
                let reduce_only: bool = row.get("reduce_only");
                let post_only: bool = row.get("post_only");

                let account_client_clone = account_client.clone();
                let risk_client_clone = risk_client.clone();
                let order_producer_clone = order_producer.clone();
                let db_pool_clone = db_pool.clone();

                tokio::spawn(async move {
                    tracing::info!("Triggering conditional order {} for user {}", order_id, user_id);

                    if reduce_only {
                        let target_pos_side = if side == "BUY" { "SHORT" } else { "LONG" };
                        let pos_opt = match sqlx::query("SELECT size FROM positions WHERE user_id = $1 AND symbol = $2 AND side = $3")
                            .bind(user_id)
                            .bind(&symbol)
                            .bind(target_pos_side)
                            .fetch_optional(&db_pool_clone)
                            .await
                        {
                            Ok(res) => res,
                            Err(e) => {
                                tracing::error!("Failed to query positions for triggered reduce-only order {}: {:?}", order_id, e);
                                let _ = sqlx::query("UPDATE orders SET status = 'FAILED', updated_at = NOW() WHERE id = $1")
                                    .bind(order_id)
                                    .execute(&db_pool_clone)
                                    .await;
                                return;
                            }
                        };

                        let position_size = match pos_opt {
                            Some(row) => row.get::<Decimal, _>("size"),
                            None => Decimal::ZERO,
                        };

                        if position_size.is_zero() {
                            tracing::warn!("Triggered reduce-only order {} rejected: no active opposite position found", order_id);
                            let _ = sqlx::query("UPDATE orders SET status = 'REJECTED', updated_at = NOW() WHERE id = $1")
                                .bind(order_id)
                                .execute(&db_pool_clone)
                                .await;
                            return;
                        }

                        let open_orders_res = match sqlx::query("SELECT quantity FROM orders WHERE user_id = $1 AND symbol = $2 AND side = $3 AND status = 'OPEN' AND reduce_only = true")
                            .bind(user_id)
                            .bind(&symbol)
                            .bind(&side)
                            .fetch_all(&db_pool_clone)
                            .await
                        {
                            Ok(res) => res,
                            Err(e) => {
                                tracing::error!("Failed to query open reduce-only orders for triggered order {}: {:?}", order_id, e);
                                let _ = sqlx::query("UPDATE orders SET status = 'FAILED', updated_at = NOW() WHERE id = $1")
                                    .bind(order_id)
                                    .execute(&db_pool_clone)
                                    .await;
                                return;
                            }
                        };

                        let existing_reduce_only_qty: Decimal = open_orders_res.iter()
                            .map(|r| r.get::<Decimal, _>("quantity"))
                            .sum();

                        if existing_reduce_only_qty + quantity > position_size {
                            tracing::warn!("Triggered reduce-only order {} rejected: quantity {} exceeds remaining position size {}", order_id, quantity, position_size - existing_reduce_only_qty);
                            let _ = sqlx::query("UPDATE orders SET status = 'REJECTED', updated_at = NOW() WHERE id = $1")
                                .bind(order_id)
                                .execute(&db_pool_clone)
                                .await;
                            return;
                        }
                    }

                    let check_res = match risk_client_clone
                        .check_order_margin(
                            user_id.to_string(),
                            symbol.clone(),
                            side.clone(),
                            quantity.to_string(),
                            price.to_string(),
                            leverage as u32,
                            "CROSS".to_string(),
                        )
                        .await
                    {
                        Ok(res) => res,
                        Err(e) => {
                            tracing::error!("Risk check failed for triggered order {}: {:?}", order_id, e);
                            let _ = sqlx::query("UPDATE orders SET status = 'FAILED', updated_at = NOW() WHERE id = $1")
                                .bind(order_id)
                                .execute(&db_pool_clone)
                                .await;
                            return;
                        }
                    };

                    if !check_res.approved {
                        tracing::warn!("Triggered order {} failed risk check: {:?}", order_id, check_res.rejection_reason);
                        let _ = sqlx::query("UPDATE orders SET status = 'REJECTED', updated_at = NOW() WHERE id = $1")
                            .bind(order_id)
                            .execute(&db_pool_clone)
                            .await;
                        return;
                    }

                    if let Err(e) = account_client_clone
                        .lock_margin(
                            user_id.to_string(),
                            check_res.required_margin.clone(),
                            order_id.to_string(),
                        )
                        .await
                    {
                        tracing::error!("Margin lock failed for triggered order {}: {:?}", order_id, e);
                        let _ = sqlx::query("UPDATE orders SET status = 'FAILED', updated_at = NOW() WHERE id = $1")
                            .bind(order_id)
                            .execute(&db_pool_clone)
                            .await;
                        return;
                    }

                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as u64;

                    let kafka_event = KafkaOrderEvent {
                        id: order_id,
                        user_id,
                        symbol: symbol.clone(),
                        side: side.clone(),
                        order_type: order_type.clone(),
                        price: price.to_string(),
                        quantity: quantity.to_string(),
                        action: "PLACE".to_string(),
                        timestamp,
                        leverage: leverage as u32,
                        reduce_only,
                        post_only,
                    };

                    if let Err(publish_err) = order_producer_clone.publish_order(&kafka_event).await {
                        tracing::error!("Failed to publish triggered order {} to Kafka: {:?}", order_id, publish_err);
                        let _ = account_client_clone
                            .release_margin(
                                user_id.to_string(),
                                check_res.required_margin,
                                order_id.to_string(),
                            )
                            .await;

                        let _ = sqlx::query("UPDATE orders SET status = 'FAILED', updated_at = NOW() WHERE id = $1")
                            .bind(order_id)
                            .execute(&db_pool_clone)
                            .await;
                    } else {
                        tracing::info!("Triggered order {} successfully published to matching engine", order_id);
                    }
                });
            }
        }
    });
}
