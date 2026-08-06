use crate::{
    application::usecase::position_usecase::PositionUseCase,
    domain::price_tracker::PriceTracker,
    domain::repositories::{
        order_repository::{OrderEntity, OrderRepository},
        trade_repository::TradeRepository,
    },
    infrastructure::{
        cache::market_cache::MarketCache,
        grpc::{account_client::AccountGrpcClient, risk_client::RiskGrpcClient},
        kafka::producer::{KafkaOrderEvent, OrderProducer},
    },
};
use proto::trading::{
    AdjustPositionMarginRequest, AdjustPositionMarginResponse, CancelOrderRequest,
    CancelOrderResponse, GetOpenOrdersRequest, GetOpenOrdersResponse, GetPositionsResponse,
    GetPostionsRequest, GetTradeHistoryRequest, GetTradeHistoryResponse, OrderInfo,
    PlaceOrderRequest, PlaceOrderResponse, PositionInfo, TradeInfo,
    trading_service_server::TradingService as GrpcTradingService,
};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use uuid::Uuid;

pub struct TradingGrpcService {
    pub position_service: Arc<dyn PositionUseCase>,
    pub account_client: AccountGrpcClient,
    pub risk_client: RiskGrpcClient,
    pub order_producer: Arc<OrderProducer>,
    pub order_repository: Arc<dyn OrderRepository>,
    pub trade_repository: Arc<dyn TradeRepository>,
    pub market_cache: Arc<MarketCache>,
    pub price_tracker: PriceTracker,
}

#[tonic::async_trait]
impl GrpcTradingService for TradingGrpcService {
    async fn place_order(
        &self,
        request: Request<PlaceOrderRequest>,
    ) -> Result<Response<PlaceOrderResponse>, Status> {
        let start_time = std::time::Instant::now();
        let req = request.into_inner();

        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let market = match self.market_cache.get(&req.symbol).await {
            Some(m) => m,
            None => {
                return Ok(Response::new(PlaceOrderResponse {
                    order_id: "".to_string(),
                    status: "REJECTED".to_string(),
                    error_message: Some(format!("Market {} not found", req.symbol)),
                }));
            }
        };

        if req.leverage > market.max_leverage {
            return Ok(Response::new(PlaceOrderResponse {
                order_id: "".to_string(),
                status: "REJECTED".to_string(),
                error_message: Some(format!(
                    "Requested leverage {} exceeds maximum allowed leverage of {} for symbol {}",
                    req.leverage, market.max_leverage, req.symbol
                )),
            }));
        }

        if req.leverage >= 200 {
            return Ok(Response::new(PlaceOrderResponse {
                order_id: "".to_string(),
                status: "REJECTED".to_string(),
                error_message: Some(
                    "Leverage must be strictly less than 200 to avoid instant liquidation"
                        .to_string(),
                ),
            }));
        }

        let new_qty = Decimal::from_str(&req.quantity)
            .map_err(|e| Status::invalid_argument(format!("Invalid quantity: {}", e)))?;

        if req.reduce_only {
            let target_pos_side = if req.side == "BUY" { "SHORT" } else { "LONG" };
            let positions = self
                .position_service
                .list_positions(user_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            let active_pos = positions.iter().find(|p| {
                p.symbol == req.symbol && p.side == target_pos_side && p.size > Decimal::ZERO
            });

            if active_pos.is_none() {
                return Ok(Response::new(PlaceOrderResponse {
                    order_id: "".to_string(),
                    status: "REJECTED".to_string(),
                    error_message: Some(
                        "Reduce-only order requires an active open position on the opposite side"
                            .to_string(),
                    ),
                }));
            }

            let position_size = active_pos.unwrap().size;

            let open_orders = self
                .order_repository
                .list_open_by_user(user_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            let existing_reduce_only_qty: Decimal = open_orders
                .iter()
                .filter(|o| o.symbol == req.symbol && o.side == req.side && o.reduce_only)
                .map(|o| o.quantity)
                .sum();

            if existing_reduce_only_qty + new_qty > position_size {
                return Ok(Response::new(PlaceOrderResponse {
                    order_id: "".to_string(),
                    status: "REJECTED".to_string(),
                    error_message: Some(format!(
                        "Reduce-only order quantity exceeds active position size. Position size: {}, Open reduce-only quantity: {}",
                        position_size, existing_reduce_only_qty
                    )),
                }));
            }
        }

        let is_stop_order = req.order_type == "STOP_MARKET" || req.order_type == "STOP_LIMIT";

        let (trigger_price_val, trigger_direction) = if is_stop_order {
            if req.trigger_price.is_none() {
                return Ok(Response::new(PlaceOrderResponse {
                    order_id: "".to_string(),
                    status: "REJECTED".to_string(),
                    error_message: Some(
                        "Trigger price is required for StopMarket and StopLimit orders".to_string(),
                    ),
                }));
            }
            let tp_val = Decimal::from_str(req.trigger_price.as_ref().unwrap())
                .map_err(|e| Status::invalid_argument(format!("Invalid trigger_price: {}", e)))?;
            let current_price = self
                .price_tracker
                .get_price(&req.symbol)
                .await
                .unwrap_or(tp_val);
            let dir = if tp_val > current_price {
                "ABOVE".to_string()
            } else {
                "BELOW".to_string()
            };
            (Some(tp_val), Some(dir))
        } else {
            (None, None)
        };

        let price_str = req.price.clone().unwrap_or_else(|| "0.00".to_string());

        let order_id = Uuid::new_v4();

        if is_stop_order {
            let order_entity = OrderEntity {
                id: order_id,
                user_id,
                symbol: req.symbol.clone(),
                side: req.side.clone(),
                order_type: req.order_type.clone(),
                price: rust_decimal::Decimal::from_str(&price_str)
                    .unwrap_or(rust_decimal::Decimal::ZERO),
                quantity: new_qty,
                status: "PENDING_TRIGGER".to_string(),
                leverage: req.leverage as i32,
                trigger_price: trigger_price_val,
                trigger_direction,
                reduce_only: req.reduce_only,
                margin_mode: req.margin_mode.clone(),
                post_only: req.post_only,
            };

            self.order_repository
                .create(order_entity)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            return Ok(Response::new(PlaceOrderResponse {
                order_id: order_id.to_string(),
                status: "PENDING_TRIGGER".to_string(),
                error_message: None,
            }));
        }

        let risk_start = std::time::Instant::now();
        let check_res = self
            .risk_client
            .check_order_margin(
                req.user_id.clone(),
                req.symbol.clone(),
                req.side.clone(),
                req.quantity.clone(),
                price_str.clone(),
                req.leverage,
                req.margin_mode.clone(),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        telemetry::metrics::TRADING_RISK_CHECK_DURATION_SECONDS
            .with_label_values(&[&req.symbol])
            .observe(risk_start.elapsed().as_secs_f64());
        tracing::info!(
            "Risk check for {} took {:?}",
            req.symbol,
            risk_start.elapsed()
        );

        if !check_res.approved {
            return Ok(Response::new(PlaceOrderResponse {
                order_id: "".to_string(),
                status: "REJECTED".to_string(),
                error_message: Some(
                    check_res
                        .rejection_reason
                        .unwrap_or_else(|| "Rejected by risk engine".to_string()),
                ),
            }));
        }

        let order_entity = OrderEntity {
            id: order_id,
            user_id,
            symbol: req.symbol.clone(),
            side: req.side.clone(),
            order_type: req.order_type.clone(),
            price: rust_decimal::Decimal::from_str(&price_str)
                .unwrap_or(rust_decimal::Decimal::ZERO),
            quantity: rust_decimal::Decimal::from_str(&req.quantity)
                .unwrap_or(rust_decimal::Decimal::ZERO),
            status: "OPEN".to_string(),
            leverage: req.leverage as i32,
            trigger_price: None,
            trigger_direction: None,
            reduce_only: req.reduce_only,
            margin_mode: req.margin_mode.clone(),
            post_only: req.post_only,
        };

        let db_start = std::time::Instant::now();
        self.order_repository
            .create(order_entity)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        telemetry::metrics::TRADING_DB_INSERT_DURATION_SECONDS
            .with_label_values(&[&req.symbol])
            .observe(db_start.elapsed().as_secs_f64());
        tracing::info!("DB insert for {} took {:?}", req.symbol, db_start.elapsed());

        let account_start = std::time::Instant::now();
        self.account_client
            .lock_margin(
                req.user_id.clone(),
                check_res.required_margin.clone(),
                order_id.to_string(),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        telemetry::metrics::TRADING_MARGIN_LOCK_DURATION_SECONDS
            .with_label_values(&[&req.symbol])
            .observe(account_start.elapsed().as_secs_f64());
        tracing::info!(
            "Margin lock for {} took {:?}",
            req.symbol,
            account_start.elapsed()
        );

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let kafka_event = KafkaOrderEvent {
            id: order_id,
            user_id,
            symbol: req.symbol.clone(),
            side: req.side.clone(),
            order_type: req.order_type.clone(),
            price: price_str.to_string(),
            quantity: req.quantity.clone(),
            action: "PLACE".to_string(),
            timestamp,
            leverage: req.leverage,
            reduce_only: req.reduce_only,
            post_only: req.post_only,
        };

        let kafka_start = std::time::Instant::now();
        if let Err(publish_err) = self.order_producer.publish_order(&kafka_event).await {
            tracing::error!(
                "Failed to publish order {} to Kafka: {}. Executing SAGA compensating rollback...",
                order_id,
                publish_err
            );

            if let Err(rollback_err) = self
                .account_client
                .release_margin(
                    req.user_id.clone(),
                    check_res.required_margin.clone(),
                    order_id.to_string(),
                )
                .await
            {
                tracing::error!(
                    "SAGA CRITICAL ERROR: Failed to release margin rollback for user {} order {}: {:?}",
                    req.user_id,
                    order_id,
                    rollback_err
                );
            }

            if let Err(db_err) = self
                .order_repository
                .update_status(order_id, "FAILED")
                .await
            {
                tracing::error!(
                    "SAGA ERROR: Failed to mark order status as FAILED for order {}: {:?}",
                    order_id,
                    db_err
                );
            }

            return Err(Status::internal(format!(
                "Failed to submit order to matching engine: {}",
                publish_err
            )));
        }
        telemetry::metrics::TRADING_KAFKA_PUBLISH_DURATION_SECONDS
            .with_label_values(&[&req.symbol])
            .observe(kafka_start.elapsed().as_secs_f64());
        tracing::info!(
            "Kafka publish for {} took {:?}",
            req.symbol,
            kafka_start.elapsed()
        );

        let elapsed = start_time.elapsed();
        tracing::info!(
            " Order {} processed and published to Kafka in {:?}",
            order_id,
            elapsed
        );

        Ok(Response::new(PlaceOrderResponse {
            order_id: order_id.to_string(),
            status: "OPEN".to_string(),
            error_message: None,
        }))
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<CancelOrderResponse>, Status> {
        let req = request.into_inner();

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let kafka_event = KafkaOrderEvent {
            id: Uuid::parse_str(&req.order_id).unwrap_or_default(),
            user_id: Uuid::parse_str(&req.user_id).unwrap_or_default(),
            symbol: req.symbol.clone(),
            side: "BUY".to_string(),
            order_type: "LIMIT".to_string(),
            price: "0.00".to_string(),
            quantity: "0.00".to_string(),
            action: "CANCEL".to_string(),
            timestamp,
            leverage: 0,
            reduce_only: false,
            post_only: false,
        };

        self.order_producer
            .publish_order(&kafka_event)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(CancelOrderResponse {
            success: true,
            error_message: None,
        }))
    }

    async fn get_postions(
        &self,
        request: Request<GetPostionsRequest>,
    ) -> Result<Response<GetPositionsResponse>, Status> {
        let req = request.into_inner();
        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let positions = self
            .position_service
            .list_positions(user_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let pb_positions = positions
            .into_iter()
            .map(|p| PositionInfo {
                symbol: p.symbol,
                side: p.side,
                size: p.size.to_string(),
                entry_price: p.entry_price.to_string(),
                leverage: p.leverage.to_string(),
                margin_mode: p.margin_mode,
                unrealized_pnl: p.unrealized_pnl.to_string(),
            })
            .collect();

        Ok(Response::new(GetPositionsResponse {
            positions: pb_positions,
        }))
    }

    async fn get_open_orders(
        &self,
        request: Request<GetOpenOrdersRequest>,
    ) -> Result<Response<GetOpenOrdersResponse>, Status> {
        let req = request.into_inner();
        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let orders = self
            .order_repository
            .list_open_by_user(user_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let pb_orders = orders
            .into_iter()
            .map(|o| OrderInfo {
                order_id: o.id.to_string(),
                symbol: o.symbol,
                side: o.side,
                order_type: o.order_type,
                price: o.price.to_string(),
                quantity: o.quantity.to_string(),
                status: o.status,
            })
            .collect();

        Ok(Response::new(GetOpenOrdersResponse { orders: pb_orders }))
    }

    async fn get_trade_history(
        &self,
        request: Request<GetTradeHistoryRequest>,
    ) -> Result<Response<GetTradeHistoryResponse>, Status> {
        let req = request.into_inner();
        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let trades = self
            .trade_repository
            .list_by_user(user_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let pb_trades = trades
            .into_iter()
            .map(|t| TradeInfo {
                id: t.id.to_string(),
                order_id: t.order_id.to_string(),
                symbol: t.symbol,
                side: t.side,
                price: t.price.to_string(),
                quantity: t.quantity.to_string(),
                fee: t.fee.to_string(),
                executed_at: t.executed_at.to_rfc3339(),
            })
            .collect();

        Ok(Response::new(GetTradeHistoryResponse { trades: pb_trades }))
    }

    async fn adjust_position_margin(
        &self,
        request: Request<AdjustPositionMarginRequest>,
    ) -> Result<Response<AdjustPositionMarginResponse>, Status> {
        let req = request.into_inner();
        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let amount =
            Decimal::from_str(&req.amount).map_err(|e| Status::invalid_argument(e.to_string()))?;

        match self
            .position_service
            .adjust_isolated_margin(user_id, &req.symbol, &req.side, amount, req.is_add)
            .await
        {
            Ok(pos) => Ok(Response::new(AdjustPositionMarginResponse {
                success: true,
                new_margin: pos.margin.to_string(),
                new_liquidation_price: pos.liquidation_price.to_string(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}
