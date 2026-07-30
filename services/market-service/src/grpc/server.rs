use std::sync::Arc;

use tonic::{Request, Response, Status};

use proto::market::{
    market_service_server::MarketService,
    GetMarketRequest,
    ListMarketsRequest,
    ListMarketsResponse,
    GetMarketResponse,
};

use crate::application::{usecase::market_usecase::MarketUseCase};

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
        
    
        let market = self
            .service
            .get_market(&symbol)
            .await
            .map_err(|e| {
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
}

