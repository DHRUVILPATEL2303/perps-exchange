use async_trait::async_trait;
use errors::app_error::RepositoryError;
use crate::domain::entities::price_tick::PriceTick;

#[async_trait]
pub trait PricePublisher: Send + Sync {
    async fn publish(&self, tick: &PriceTick) -> Result<(), RepositoryError>;
}
