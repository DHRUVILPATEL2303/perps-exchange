use anyhow::Result;
use tonic::transport::{Channel, Endpoint};
use proto::trading::{
    trading_service_client::TradingServiceClient, GetPostionsRequest, GetPositionsResponse,
};

#[derive(Clone)]
pub struct TradingGrpcClient {
    client: TradingServiceClient<Channel>,
}

impl TradingGrpcClient {
    pub async fn connect(endpoint: String) -> Result<Self> {
        let endpoint = Endpoint::from_shared(endpoint)?;
        let channel = endpoint.connect_lazy();
        let client = TradingServiceClient::new(channel);
        Ok(Self { client })
    }

    pub async fn get_positions(&self, user_id: String) -> Result<GetPositionsResponse> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(GetPostionsRequest { user_id });
        let response = client.get_postions(request).await?;
        Ok(response.into_inner())
    }
}