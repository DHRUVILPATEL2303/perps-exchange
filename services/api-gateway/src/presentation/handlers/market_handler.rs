use actix_web::web::Data;
use actix_web::HttpResponse;
use crate::state::AppState;
use proto::market::ListMarketsRequest;

pub async fn list_markets(state: Data<AppState>) -> HttpResponse {
    let mut client = state.market_client.clone();
    match client.list_markets(ListMarketsRequest {}).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner().markets),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}
