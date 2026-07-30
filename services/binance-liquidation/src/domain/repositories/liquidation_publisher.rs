
use errors::app_error::RepositoryError;

use crate::domain::entities::liquidation::Liquidation;

#[async_trait::async_trait]
pub trait LiquidationPublisher: Send + Sync {
    async fn publish(&self, liquidation: &Liquidation) -> Result<(), RepositoryError>;
}
