use proto::market::{ListMarketsRequest, market_service_client::MarketServiceClient};
use tonic::transport::{Channel, Endpoint};

use crate::domain::entities::market::Market;

pub struct MarketGrpcClient {
    pub client: MarketServiceClient<Channel>,
}

impl MarketGrpcClient {
    pub async fn connect(endpoint: String) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint)?.connect().await?;

        Ok(Self {
            client: MarketServiceClient::new(channel),
        })
    }

    pub async fn list_markets(&mut self) -> anyhow::Result<Vec<Market>> {
        let response = self
            .client
            .list_markets(ListMarketsRequest {})
            .await?
            .into_inner();

        let markets = response
            .markets
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(markets)
    }
}
