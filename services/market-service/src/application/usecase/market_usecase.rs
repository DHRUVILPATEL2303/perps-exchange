use async_trait::async_trait;
use errors::app_error::ServiceError;

use crate::application::dto::{requests::{create_market_request::CreateMarketRequest, update_market_request::UpdateMarketRequest}, response::market_response::MarketResponse};

#[async_trait]
pub trait MarketUseCase: Send + Sync {
    async fn list_markets(
        &self,
    ) -> Result<Vec<MarketResponse>, ServiceError>;

    async fn get_market(
        &self,
        symbol: &str,
    ) -> Result<Option<MarketResponse>, ServiceError>;

    async fn create_market(
        &self,
        request: CreateMarketRequest,
    ) -> Result<MarketResponse, ServiceError>;

    async fn update_market(
        &self,
        symbol : &str,
        request : UpdateMarketRequest
    ) -> Result<MarketResponse, ServiceError>;
}