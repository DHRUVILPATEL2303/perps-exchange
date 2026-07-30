use std::sync::Arc;

use async_trait::async_trait;
use errors::app_error::ServiceError;
use rust_decimal::Decimal;

use crate::{application::{dto::{requests::create_market_request::CreateMarketRequest, response::market_response::MarketResponse}, usecase::market_usecase::MarketUseCase}, domain::repositories::market_repository::MarketRepository};


pub struct MarketService {
    repository : Arc<dyn MarketRepository>
}

impl MarketService {
    pub fn new(repository: Arc<dyn MarketRepository>) -> Self {
        Self { repository: repository }
    }
    
}
fn validate_request(
    request: &CreateMarketRequest,
) -> Result<(), ServiceError> {
    if request.tick_size <= Decimal::ZERO {
        return Err(ServiceError::InvalidTickSize);
    }

    if request.lot_size <= Decimal::ZERO {
        return Err(ServiceError::InvalidLotSize);
    }

    if request.max_leverage == 0 {
        return Err(ServiceError::InvalidLeverage);
    }

    Ok(())
}


#[async_trait]
impl MarketUseCase for MarketService {
    async fn list_markets(
        &self,
    ) -> Result<Vec<MarketResponse>, ServiceError> {

        let markets = self.repository.list().await?;

        Ok(markets.into_iter().map(|m| m.into()).collect())
    }

    async fn get_market(
        &self,
        symbol: &str,
    ) -> Result<Option<MarketResponse>, ServiceError> {
    
        let market = self
            .repository
            .find_by_symbol(symbol)
            .await?;
    
        Ok(market.map(Into::into))
    }

    async fn create_market(
        &self,
        request: CreateMarketRequest,
    ) -> Result<MarketResponse, ServiceError> {
        todo!()
    }
}