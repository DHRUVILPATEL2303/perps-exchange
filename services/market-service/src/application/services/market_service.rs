use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use errors::app_error::ServiceError;
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::{
    application::{
        dto::{
            requests::{
                create_market_request::CreateMarketRequest,
                update_market_request::UpdateMarketRequest,
            },
            response::market_response::MarketResponse,
        },
        usecase::market_usecase::MarketUseCase,
    },
    domain::{entities::market::Market, repositories::market_repository::MarketRepository},
};

pub struct MarketService {
    repository: Arc<dyn MarketRepository>,
}

impl MarketService {
    pub fn new(repository: Arc<dyn MarketRepository>) -> Self {
        Self { repository }
    }

    fn validate_request(request: &CreateMarketRequest) -> Result<(), ServiceError> {
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

    fn validate_update_request(request: &UpdateMarketRequest) -> Result<(), ServiceError> {
        if request.tick_size <= Decimal::ZERO {
            return Err(ServiceError::InvalidTickSize);
        }

        if request.lot_size <= Decimal::ZERO {
            return Err(ServiceError::InvalidLotSize);
        }

        if request.max_leverage == 0 {
            return Err(ServiceError::InvalidLeverage);
        }

        match request.status.as_str() {
            "ACTIVE" | "PAUSED" | "DISABLED" => {}
            _ => return Err(ServiceError::InvalidStatus),
        }

        Ok(())
    }
}

#[async_trait]
impl MarketUseCase for MarketService {
    async fn list_markets(&self) -> Result<Vec<MarketResponse>, ServiceError> {
        let markets = self.repository.list().await?;

        Ok(markets.into_iter().map(|m| m.into()).collect())
    }

    async fn get_market(&self, symbol: &str) -> Result<Option<MarketResponse>, ServiceError> {
        let market = self.repository.find_by_symbol(symbol).await?;

        Ok(market.map(Into::into))
    }
    async fn create_market(
        &self,
        request: CreateMarketRequest,
    ) -> Result<MarketResponse, ServiceError> {
        Self::validate_request(&request)?;

        if self
            .repository
            .find_by_symbol(&request.symbol)
            .await?
            .is_some()
        {
            return Err(ServiceError::MarketAlreadyExists);
        }

        let market = Market {
            id: Uuid::new_v4(),
            symbol: request.symbol,
            base_asset: request.base_asset,
            quote_asset: request.quote_asset,
            tick_size: request.tick_size,
            lot_size: request.lot_size,
            min_qty: request.min_qty,
            max_leverage: request.max_leverage as i32,
            status: "ACTIVE".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let market = self.repository.create(market).await?;

        Ok(market.into())
    }

    async fn update_market(
        &self,
        symbol: &str,
        request: UpdateMarketRequest,
    ) -> Result<MarketResponse, ServiceError> {
        Self::validate_update_request(&request)?;

        let mut market = self
            .repository
            .find_by_symbol(symbol)
            .await?
            .ok_or(ServiceError::MarketNotFound)?;

        market.tick_size = request.tick_size;
        market.lot_size = request.lot_size;
        market.min_qty = request.min_qty;
        market.max_leverage = request.max_leverage as i32;
        market.status = request.status;
        market.updated_at = Utc::now();

        let market = self.repository.update(market).await?;

        Ok(market.into())
    }

    async fn delete_market(&self, symbol: &str) -> Result<(), ServiceError> {
        self.repository.delete_by_symbol(symbol).await?;
        Ok(())
    }
}
