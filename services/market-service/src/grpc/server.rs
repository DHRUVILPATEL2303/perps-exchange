use std::sync::Arc;

use tonic::{Request, Response, Status};

use proto::market::{
    GetMarketRequest, GetMarketResponse, ListMarketsRequest, ListMarketsResponse,
    CreateMarketRequest, market_service_server::MarketService,
};

use crate::application::usecase::market_usecase::MarketUseCase;

pub struct MarketGrpcService {
    pub service: Arc<dyn MarketUseCase>,
}

#[tonic::async_trait]
impl MarketService for MarketGrpcService {
    async fn get_market(
        &self,
        request: Request<GetMarketRequest>,
    ) -> Result<Response<GetMarketResponse>, Status> {
        let symbol = request.into_inner().symbol;

        let market = self.service.get_market(&symbol).await.map_err(|e| {
            println!("Service error: {}", e);
            Status::internal(e.to_string())
        })?;

        let market = market.ok_or_else(|| {
            println!("Market not found");
            Status::not_found("Market not found")
        })?;

        Ok(Response::new(market.into()))
    }

    async fn list_markets(
        &self,
        _request: Request<ListMarketsRequest>,
    ) -> Result<Response<ListMarketsResponse>, Status> {
        let markets = self
            .service
            .list_markets()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let response = ListMarketsResponse {
            markets: markets.into_iter().map(Into::into).collect(),
        };

        Ok(Response::new(response))
    }

    async fn create_market(
        &self,
        request: Request<CreateMarketRequest>,
    ) -> Result<Response<GetMarketResponse>, Status> {
        let req = request.into_inner();

        let tick_size = req.tick_size.parse::<rust_decimal::Decimal>().map_err(|e| {
            Status::invalid_argument(format!("Invalid tick_size: {}", e))
        })?;
        let lot_size = req.lot_size.parse::<rust_decimal::Decimal>().map_err(|e| {
            Status::invalid_argument(format!("Invalid lot_size: {}", e))
        })?;
        let min_qty = req.min_qty.parse::<rust_decimal::Decimal>().map_err(|e| {
            Status::invalid_argument(format!("Invalid min_qty: {}", e))
        })?;

        let create_dto = crate::application::dto::requests::create_market_request::CreateMarketRequest {
            symbol: req.symbol,
            base_asset: req.base_asset,
            quote_asset: req.quote_asset,
            tick_size,
            lot_size,
            min_qty,
            max_leverage: req.max_leverage as u16,
        };

        let response = self.service.create_market(create_dto).await.map_err(|e| {
            Status::internal(e.to_string())
        })?;

        Ok(Response::new(response.into()))
    }
}
