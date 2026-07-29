use std::sync::Arc;

use crate::{
    application::dto::health_response::HealthResponse,
    domain::repositories::health_repository::HealthRepository,
};

pub struct HealthService {
    health_repository: Arc<dyn HealthRepository>,
}

impl HealthService {
    pub fn new(repository: Arc<dyn HealthRepository>) -> Self {
        Self {
            health_repository: repository,
        }
    }

    pub async fn health(&self) -> HealthResponse {
        let service = self.health_repository.service_name().await;
        HealthResponse {
            status: "healthy".into(),
            service,
        }
    }
}
