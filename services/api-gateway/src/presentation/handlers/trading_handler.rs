use crate::presentation::handlers::auth_handler::AuthenticatedUser;
use crate::state::AppState;
use actix_web::HttpResponse;
use actix_web::web::{Data, Json, Path, Query};
use proto::trading::{
    CancelOrderRequest, GetOpenOrdersRequest, GetPostionsRequest, PlaceOrderRequest,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct PaginationQuery {
    pub page: Option<i32>,
    pub limit: Option<i32>,
}

#[derive(Deserialize, Serialize)]
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

pub async fn get_positions(
    state: Data<AppState>,
    path: Path<Uuid>,
    user: AuthenticatedUser,
) -> HttpResponse {
    let user_id = path.into_inner();
    if user.user_id != user_id.to_string() {
        return HttpResponse::Forbidden().body("Access denied");
    }
    let idx = state
        .trading_pool_index
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        % state.trading_clients.len();
    let mut client = state.trading_clients[idx].clone();
    let req = GetPostionsRequest {
        user_id: user_id.to_string(),
    };
    match client.get_postions(req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner().positions),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn place_order(
    state: Data<AppState>,
    body: Json<HTTPPlaceOrderRequest>,
    user: AuthenticatedUser,
) -> HttpResponse {
    let req = body.into_inner();
    if user.user_id != req.user_id.to_string() {
        return HttpResponse::Forbidden().body("Access denied");
    }
    let idx = state
        .trading_pool_index
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        % state.trading_clients.len();
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

pub async fn cancel_order(
    state: Data<AppState>,
    body: Json<HTTPCancelOrderRequest>,
    user: AuthenticatedUser,
) -> HttpResponse {
    let req = body.into_inner();
    if user.user_id != req.user_id.to_string() {
        return HttpResponse::Forbidden().body("Access denied");
    }
    let idx = state
        .trading_pool_index
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        % state.trading_clients.len();
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

pub async fn get_open_orders(
    state: Data<AppState>,
    path: Path<Uuid>,
    query: Query<PaginationQuery>,
    user: AuthenticatedUser,
) -> HttpResponse {
    let user_id = path.into_inner();
    if user.user_id != user_id.to_string() {
        return HttpResponse::Forbidden().body("Access denied");
    }
    let idx = state
        .trading_pool_index
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        % state.trading_clients.len();
    let mut client = state.trading_clients[idx].clone();
    let grpc_req = GetOpenOrdersRequest {
        user_id: user_id.to_string(),
        page: query.page,
        limit: query.limit,
    };
    match client.get_open_orders(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner().orders),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn get_trade_history(
    state: Data<AppState>,
    path: Path<Uuid>,
    query: Query<PaginationQuery>,
    user: AuthenticatedUser,
) -> HttpResponse {
    let user_id = path.into_inner();
    if user.user_id != user_id.to_string() {
        return HttpResponse::Forbidden().body("Access denied");
    }
    let idx = state
        .trading_pool_index
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        % state.trading_clients.len();
    let mut client = state.trading_clients[idx].clone();
    let grpc_req = proto::trading::GetTradeHistoryRequest {
        user_id: user_id.to_string(),
        page: query.page,
        limit: query.limit,
    };
    match client.get_trade_history(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner().trades),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[derive(Deserialize)]
pub struct HTTPAdjustPositionMarginRequest {
    pub user_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub amount: String,
    pub is_add: bool,
}

pub async fn adjust_position_margin(
    state: Data<AppState>,
    body: Json<HTTPAdjustPositionMarginRequest>,
    user: AuthenticatedUser,
) -> HttpResponse {
    let req = body.into_inner();
    if user.user_id != req.user_id.to_string() {
        return HttpResponse::Forbidden().body("Access denied");
    }
    let idx = state
        .trading_pool_index
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        % state.trading_clients.len();
    let mut client = state.trading_clients[idx].clone();
    let grpc_req = proto::trading::AdjustPositionMarginRequest {
        user_id: req.user_id.to_string(),
        symbol: req.symbol,
        side: req.side,
        amount: req.amount,
        is_add: req.is_add,
    };
    match client.adjust_position_margin(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}
