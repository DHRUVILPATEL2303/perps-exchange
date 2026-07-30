use sqlx::PgPool;

use crate::config::DatabaseConfig;
use crate::pool::create_pool;

#[derive(Debug, Clone)]
pub struct DatabaseManager {
    pub pool: PgPool,
}

impl DatabaseManager {
    pub async fn new(config: &DatabaseConfig) -> Result<Self, sqlx::Error> {
        let pool = create_pool(config).await?;

        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}
