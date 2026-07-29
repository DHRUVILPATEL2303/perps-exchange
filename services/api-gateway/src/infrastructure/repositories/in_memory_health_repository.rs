use crate::domain::repositories::health_repository::HealthRepository;
use async_trait::async_trait;

pub struct InMemoryHealthRepository {
    service_name : String
}

impl InMemoryHealthRepository {
    pub fn new(service_name: String) -> Self {
        Self { service_name }
    }
    
}

#[async_trait]
impl HealthRepository for InMemoryHealthRepository {
    async fn service_name(&self) -> String {
        self.service_name.clone()
    }
}