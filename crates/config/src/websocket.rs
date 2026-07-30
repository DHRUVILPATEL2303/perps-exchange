use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct WebsocketConfig {
    pub url: String,
}
