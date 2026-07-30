use serde::Deserialize;

use crate::{database::DatabaseConfig, grpc::GrpcConfig, server::ServerConfig};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub environment: String,
    pub app_name: String,

    pub server: ServerConfig,
    pub grpc: GrpcConfig,

    pub database: DatabaseConfig,
}
