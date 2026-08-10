use crate::state::AppState;
use actix_web::HttpResponse;
use actix_web::web::{Data, Path, Query};
use proto::chart::{GetCandlesRequest as GrpcGetCandlesRequest, GetTickerRequest, GetRecentTradesRequest};
use proto::market::{CreateMarketRequest, ListMarketsRequest};
use redis::AsyncCommands;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct HTTPCreateMarketRequest {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub tick_size: String,
    pub lot_size: String,
    pub min_qty: String,
    pub max_leverage: u32,
}

#[derive(serde::Deserialize)]
pub struct CandlesQuery {
    pub resolution: Option<String>,
    pub limit: Option<i32>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Candle {
    pub timestamp: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
}

pub async fn list_markets(state: Data<AppState>) -> HttpResponse {
    let mut client = state.market_client.clone();
    match client.list_markets(ListMarketsRequest {}).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner().markets),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn create_market(
    state: Data<AppState>,
    body: actix_web::web::Json<HTTPCreateMarketRequest>,
) -> HttpResponse {
    let req = body.into_inner();
    let mut client = state.market_client.clone();
    let grpc_req = CreateMarketRequest {
        symbol: req.symbol,
        base_asset: req.base_asset,
        quote_asset: req.quote_asset,
        tick_size: req.tick_size,
        lot_size: req.lot_size,
        min_qty: req.min_qty,
        max_leverage: req.max_leverage,
    };
    match client.create_market(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn get_candles(
    state: Data<AppState>,
    symbol: Path<String>,
    query: Query<CandlesQuery>,
) -> HttpResponse {
    let symbol = symbol.into_inner();
    let resolution = query.resolution.clone().unwrap_or_else(|| "1m".to_string());
    let limit = query.limit.unwrap_or(100);

    let key = format!("candles:{}:{}", symbol, resolution);

    let mut redis_conn = match state.redis_client.get_multiplexed_async_connection().await {
        Ok(conn) => conn,
        Err(e) => {
            return HttpResponse::InternalServerError()
                .body(format!("Redis connection failed: {:?}", e));
        }
    };

    let cached_jsons: Result<Vec<String>, _> =
        redis_conn.zrevrange(&key, 0, (limit - 1) as isize).await;

    if let Ok(jsons) = cached_jsons {
        if jsons.len() >= limit as usize {
            let mut candles = Vec::new();
            for json_str in jsons {
                if let Ok(c) = serde_json::from_str::<Candle>(&json_str) {
                    candles.push(c);
                }
            }
            candles.reverse();
            return HttpResponse::Ok().json(candles);
        }
    }

    let mut chart_client = state.chart_client.clone();
    let grpc_req = GrpcGetCandlesRequest {
        symbol: symbol.clone(),
        resolution: resolution.clone(),
        limit,
    };

    match chart_client.get_candles(grpc_req).await {
        Ok(grpc_res) => {
            let grpc_candles = grpc_res.into_inner().candles;
            let mut response_candles = Vec::new();

            for c in grpc_candles {
                let candle = Candle {
                    timestamp: c.timestamp,
                    open: c.open.clone(),
                    high: c.high.clone(),
                    low: c.low.clone(),
                    close: c.close.clone(),
                    volume: c.volume.clone(),
                };
                response_candles.push(candle.clone());

                let candle_json = match serde_json::to_string(&candle) {
                    Ok(json) => json,
                    Err(_) => continue,
                };
                let mut conn = redis_conn.clone();
                let zset_key = key.clone();
                tokio::spawn(async move {
                    let _: Result<(), _> =
                        conn.zrembyscore(&zset_key, c.timestamp, c.timestamp).await;
                    let _: Result<(), _> = conn.zadd(&zset_key, &candle_json, c.timestamp).await;
                    let _: Result<(), _> =
                        conn.zremrangebyrank(&zset_key, 0_isize, -1001_isize).await;
                });
            }

            response_candles.reverse();
            HttpResponse::Ok().json(response_candles)
        }
        Err(e) => {
            HttpResponse::InternalServerError().body(format!("Failed to retrieve candles: {:?}", e))
        }
    }
}

pub async fn get_ticker(state: Data<AppState>, path: Path<String>) -> HttpResponse {
    let symbol = path.into_inner();
    let mut client = state.chart_client.clone();
    match client.get_ticker(GetTickerRequest { symbol }).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[derive(Deserialize)]
pub struct TradesQuery {
    pub limit: Option<i32>,
}

pub async fn get_recent_trades(
    state: Data<AppState>,
    path: Path<String>,
    query: Query<TradesQuery>,
) -> HttpResponse {
    let symbol = path.into_inner();
    let limit = query.limit.unwrap_or(50);
    let mut client = state.chart_client.clone();
    match client.get_recent_trades(GetRecentTradesRequest { symbol, limit }).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner().trades),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}
