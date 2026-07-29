use sqlx::{PgPool, postgres::PgPoolOptions};




pub async fn create_pool(
    config: &config::DatabaseConfig,
) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(20)
        .connect(&config.url().to_string())
        .await
}