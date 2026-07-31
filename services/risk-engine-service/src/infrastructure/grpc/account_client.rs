use anyhow::Result;
use tonic::transport::{Channel, Endpoint};
use proto::account::{account_service_client::AccountServiceClient, GetBalanceRequest, GetBalanceResponse};

#[derive(Clone)]
pub struct AccountGrpcClient {
    client: AccountServiceClient<Channel>,
}

impl AccountGrpcClient {
    pub async fn connect(endpoint: String) -> Result<Self> {
        let endpoint = Endpoint::from_shared(endpoint)?;
        let channel = endpoint.connect_lazy();
        let client = AccountServiceClient::new(channel);
        Ok(Self { client })
    }

    pub async fn get_balance(&self, user_id: String, asset: String) -> Result<GetBalanceResponse> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(GetBalanceRequest { user_id, asset });
        let response = client.get_balance(request).await?;
        Ok(response.into_inner())
    }
}
