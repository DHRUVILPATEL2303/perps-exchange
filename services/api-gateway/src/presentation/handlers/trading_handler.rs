use actix_web::web::{Data, Json, Path};
use actix_web::HttpResponse;
use crate::state::AppState;
use proto::trading::{
    PlaceOrderRequest, CancelOrderRequest, GetPostionsRequest, GetOpenOrdersRequest,
};
use uuid::Uuid;
use serde::{Deserialize, Serialize};

#[derive(Deserialize,Serialize)]
pub struct HTTPPlaceOrderRequest {
    pub user_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub quantity: String,
    pub price: Option<String>,
    pub trigger_price: Option<String>,
    pub time_in_force: String,
    pub leverage: u32,
    pub margin_mode: String,
    pub reduce_only: bool,
    pub post_only: bool,
}

#[derive(Deserialize)]
pub struct HTTPCancelOrderRequest {
    pub user_id: Uuid,
    pub order_id: Uuid,
    pub symbol: String,
}

pub async fn get_positions(state: Data<AppState>, path: Path<Uuid>) -> HttpResponse {
    let user_id = path.into_inner();
    let idx = state.trading_pool_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % state.trading_clients.len();
    let mut client = state.trading_clients[idx].clone();
    let req = GetPostionsRequest {
        user_id: user_id.to_string(),
    };
    match client.get_postions(req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner().positions),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn place_order(state: Data<AppState>, body: Json<HTTPPlaceOrderRequest>) -> HttpResponse {
    let req = body.into_inner();
    let idx = state.trading_pool_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % state.trading_clients.len();
    let mut client = state.trading_clients[idx].clone();
    let grpc_req = PlaceOrderRequest {
        user_id: req.user_id.to_string(),
        symbol: req.symbol,
        side: req.side,
        order_type: req.order_type,
        quantity: req.quantity,
        price: req.price,
        trigger_price: req.trigger_price,
        time_in_force: req.time_in_force,
        leverage: req.leverage,
        margin_mode: req.margin_mode,
        reduce_only: req.reduce_only,
        post_only: req.post_only,
    };
    match client.place_order(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn cancel_order(state: Data<AppState>, body: Json<HTTPCancelOrderRequest>) -> HttpResponse {
    let req = body.into_inner();
    let idx = state.trading_pool_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % state.trading_clients.len();
    let mut client = state.trading_clients[idx].clone();
    let grpc_req = CancelOrderRequest {
        user_id: req.user_id.to_string(),
        order_id: req.order_id.to_string(),
        symbol: req.symbol,
    };
    match client.cancel_order(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn get_open_orders(state: Data<AppState>, path: Path<Uuid>) -> HttpResponse {
    let user_id = path.into_inner();
    let idx = state.trading_pool_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % state.trading_clients.len();
    let mut client = state.trading_clients[idx].clone();
    let grpc_req = GetOpenOrdersRequest {
        user_id: user_id.to_string(),
    };
    match client.get_open_orders(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner().orders),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn get_trade_history(state: Data<AppState>, path: Path<Uuid>) -> HttpResponse {
    let user_id = path.into_inner();
    let idx = state.trading_pool_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % state.trading_clients.len();
    let mut client = state.trading_clients[idx].clone();
    let grpc_req = proto::trading::GetTradeHistoryRequest {
        user_id: user_id.to_string(),
    };
    match client.get_trade_history(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner().trades),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}
