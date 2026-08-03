use sqlx::{PgPool, postgres::PgPoolOptions, postgres::PgConnectOptions};
use std::str::FromStr;

pub async fn create_pool(config: &config::DatabaseConfig) -> Result<PgPool, sqlx::Error> {
    let mut connect_options = PgConnectOptions::from_str(&config.url().to_string())?;
    connect_options = connect_options.statement_cache_capacity(0);

    PgPoolOptions::new()
        .max_connections(80)
        .min_connections(20)
        .acquire_timeout(std::time::Duration::from_secs(3))
        .idle_timeout(std::time::Duration::from_secs(60))
        .test_before_acquire(false)
        .connect_with(connect_options)
        .await
}
