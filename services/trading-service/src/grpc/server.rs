use std::sync::Arc;
use tonic::{Request, Response, Status};
use uuid::Uuid;
use crate::application::usecase::position_usecase::PositionUseCase;
use proto::trading::{
    trading_service_server::TradingService as GrpcTradingService,
    PlaceOrderRequest, PlaceOrderResponse,
    CancelOrderRequest, CancelOrderResponse,
    GetPostionsRequest, GetPositionsResponse,
    PositionInfo,
};

pub struct TradingGrpcService {
    pub position_service: Arc<dyn PositionUseCase>,
}

#[tonic::async_trait]
impl GrpcTradingService for TradingGrpcService {
    async fn place_order(
        &self,
        request: Request<PlaceOrderRequest>,
    ) -> Result<Response<PlaceOrderResponse>, Status> {
        let _req = request.into_inner();
        
        Ok(Response::new(PlaceOrderResponse {
            order_id: Uuid::new_v4().to_string(),
            status: "OPEN".to_string(),
            error_message: None,
        }))
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<CancelOrderResponse>, Status> {
        let _req = request.into_inner();

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
        let user_id = Uuid::parse_str(&req.user_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let positions = self.position_service.list_positions(user_id)
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
}
