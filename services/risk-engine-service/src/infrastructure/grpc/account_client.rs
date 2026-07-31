use proto::account::{GetBalanceRequest, GetBalanceResponse, account_service_client::AccountServiceClient, account_service_server::AccountService};
use tonic::transport::Channel;
use tracing::subscriber::SetGlobalDefaultError;

use anyhow::Result;
#[derive(Debug,Clone)]
pub struct AccountGrpcClient {
    pub service :AccountServiceClient<Channel>
}

impl AccountGrpcClient {
    pub  async fn  connect(endpoint : String ) -> Result<Self>  {
        let client = AccountServiceClient::connect(endpoint.clone()).await?;
        return Ok(Self {
            service : client
        })
        
    }

    pub async fn get_account_balance(&self, user_id : String , asset : String ) -> Result<GetBalanceResponse> {
        let mut  client = self.service.clone();
        let request = tonic::Request::new(GetBalanceRequest {
            user_id,
            asset
        });

        let response = client.get_balance(request).await?;
        return Ok(response.into_inner());
    }

    
}

