use serde::Deserialize;

use crate::{database::DatabaseConfig, grpc::GrpcConfig, kafka::KafkaConfig, redis::RedisConfig, server::ServerConfig, websocket::WebsocketConfig};






#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub environment: String,
    pub app_name: String,

    pub server: ServerConfig,
    pub grpc: GrpcConfig,

    pub database: DatabaseConfig,
    pub kafka: KafkaConfig,
    pub redis: RedisConfig,
    
    #[serde(default)]
    pub websocket: Option<WebsocketConfig>,
}
