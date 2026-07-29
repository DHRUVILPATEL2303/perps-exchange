use async_trait::async_trait;

#[async_trait]
pub trait HealthRepository: Send + Sync {
    async fn service_name(&self) -> String;
}
