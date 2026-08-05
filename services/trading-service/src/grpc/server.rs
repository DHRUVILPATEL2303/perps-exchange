use crate::{
    application::usecase::position_usecase::PositionUseCase,
    domain::repositories::order_repository::{OrderEntity, OrderRepository},
    infrastructure::{
        grpc::{account_client::AccountGrpcClient, risk_client::RiskGrpcClient},
        kafka::producer::{KafkaOrderEvent, OrderProducer},
    },
};
use proto::trading::{
    CancelOrderRequest, CancelOrderResponse, GetOpenOrdersRequest, GetOpenOrdersResponse,
    GetPositionsResponse, GetPostionsRequest, OrderInfo, PlaceOrderRequest, PlaceOrderResponse,
    PositionInfo, trading_service_server::TradingService as GrpcTradingService,
};
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
}

#[tonic::async_trait]
impl GrpcTradingService for TradingGrpcService {
    async fn place_order(
        &self,
        request: Request<PlaceOrderRequest>,
    ) -> Result<Response<PlaceOrderResponse>, Status> {
        let start_time = std::time::Instant::now();
        let req = request.into_inner();
        let price_str = req.price.clone().unwrap_or_else(|| "0.00".to_string());

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
        tracing::info!("Risk check for {} took {:?}", req.symbol, risk_start.elapsed());

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

        let order_id = Uuid::new_v4();
        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

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
        tracing::info!("Margin lock for {} took {:?}", req.symbol, account_start.elapsed());

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let kafka_event = KafkaOrderEvent {
            id: order_id.to_string(),
            user_id: req.user_id.clone(),
            symbol: req.symbol.clone(),
            side: req.side.clone(),
            order_type: req.order_type.clone(),
            price: price_str.to_string(),
            quantity: req.quantity.clone(),
            action: "PLACE".to_string(),
            timestamp,
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
        tracing::info!("Kafka publish for {} took {:?}", req.symbol, kafka_start.elapsed());

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
            id: req.order_id.clone(),
            user_id: req.user_id.clone(),
            symbol: req.symbol.clone(),
            side: "BUY".to_string(),
            order_type: "LIMIT".to_string(),
            price: "0.00".to_string(),
            quantity: "0.00".to_string(),
            action: "CANCEL".to_string(),
            timestamp,
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
}
