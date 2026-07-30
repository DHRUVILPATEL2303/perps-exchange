use actix_web::web::{Data, Path};
use actix_web::HttpResponse;
use crate::state::AppState;
use proto::account::GetBalanceRequest;
use uuid::Uuid;

pub async fn get_balance(state: Data<AppState>, path: Path<Uuid>) -> HttpResponse {
    let user_id = path.into_inner();
    let mut client = state.account_client.clone();
    let req = GetBalanceRequest {
        user_id: user_id.to_string(),
        asset: "USDT".to_string(),
    };
    match client.get_balance(req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

