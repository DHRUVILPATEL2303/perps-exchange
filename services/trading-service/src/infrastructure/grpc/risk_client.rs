use anyhow::Result;
use proto::risk::{
    risk_service_client::RiskServiceClient,
    CheckOrderMarginRequest, CheckOrderMarginResponse,
};
use tonic::transport::{Channel, Endpoint};

#[derive(Clone)]
pub struct RiskGrpcClient {
    client: RiskServiceClient<Channel>,
}

impl RiskGrpcClient {
    pub async fn connect(endpoint: String) -> Result<Self> {
        let endpoint = Endpoint::from_shared(endpoint)?;
        let channel = endpoint.connect_lazy();
        let client = RiskServiceClient::new(channel);
        Ok(Self { client })
    }

    pub async fn check_order_margin(
        &self,
        user_id: String,
        symbol: String,
        side: String,
        quantity: String,
        price: String,
        leverage: u32,
        margin_mode: String,
    ) -> Result<CheckOrderMarginResponse> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(CheckOrderMarginRequest {
            user_id,
            symbol,
            side,
            quantity,
            price,
            leverage,
            margin_mode,
        });
        let response = client.check_order_margin(request).await?;
        Ok(response.into_inner())
    }
}
