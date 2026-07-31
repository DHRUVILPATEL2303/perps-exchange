use anyhow::Result;
use proto::trading::{GetPositionsResponse, GetPostionsRequest, trading_service_client::TradingServiceClient};
use tonic::transport::Channel;

#[derive(Clone)]
pub struct TradingGrpcClient {
    pub service : TradingServiceClient<Channel>
}

impl TradingGrpcClient {
    pub async fn connect(endpoint : String) ->Result<Self> {
        let service = TradingServiceClient::connect(endpoint).await?;
        Ok(Self {
            service : service
        })
        
    }


    pub async fn get_positions(&self,user_id : String ) -> Result<GetPositionsResponse> {
        let mut  client = self.service.clone();
        let request = tonic::Request::new(GetPostionsRequest {
            user_id
        });

        let response = client.get_postions(request).await?;
        Ok(response.into_inner())
    }
}