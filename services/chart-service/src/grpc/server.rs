use proto::chart::{
    CandleInfo, GetCandlesRequest, GetCandlesResponse, GetTickerRequest, GetTickerResponse, chart_service_server::ChartService as GrpcChartService,
};
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};

pub struct ChartGrpcService {
    pub db_pool: PgPool,
}

#[tonic::async_trait]
impl GrpcChartService for ChartGrpcService {
    async fn get_ticker(
        &self,
        request: Request<GetTickerRequest>,
    ) -> Result<Response<GetTickerResponse>, Status> {
        let symbol = request.into_inner().symbol;

        let row = sqlx::query(
            r#"
            SELECT
                first(price, time)    AS open_24h,
                last(price, time)     AS last_price,
                MAX(price)            AS high_24h,
                MIN(price)            AS low_24h,
                SUM(quantity)         AS volume_24h,
                COUNT(*)              AS trade_count_24h
            FROM trades
            WHERE symbol = $1
              AND time >= NOW() - INTERVAL '24 hours'
            "#,
        )
        .bind(&symbol)
        .fetch_optional(&self.db_pool)
        .await
        .map_err(|e| Status::internal(format!("Ticker DB error: {:?}", e)))?;

        let response = match row {
            None => {
                let last_known = sqlx::query(
                    "SELECT price FROM trades WHERE symbol = $1 ORDER BY time DESC LIMIT 1"
                )
                .bind(&symbol)
                .fetch_optional(&self.db_pool)
                .await
                .map_err(|e| Status::internal(format!("Fallback ticker error: {:?}", e)))?;
            
                let last_price = last_known
                    .and_then(|r| r.try_get::<rust_decimal::Decimal, _>("price").ok())
                    .unwrap_or(rust_decimal::Decimal::ZERO)
                    .to_string();
            
                GetTickerResponse {
                    symbol,
                    last_price,           
                    price_change_24h: "0".to_string(),
                    price_change_pct_24h: "0".to_string(),
                    high_24h: "0".to_string(),
                    low_24h: "0".to_string(),
                    volume_24h: "0".to_string(),
                    open_24h: "0".to_string(),
                    trade_count_24h: 0,
                }
            }
,
            Some(row) => {
                let open: Option<rust_decimal::Decimal> = row.try_get("open_24h").ok();
                let last: Option<rust_decimal::Decimal> = row.try_get("last_price").ok();
                let high: Option<rust_decimal::Decimal> = row.try_get("high_24h").ok();
                let low: Option<rust_decimal::Decimal> = row.try_get("low_24h").ok();
                let volume: Option<rust_decimal::Decimal> = row.try_get("volume_24h").ok();
                let count: Option<i64> = row.try_get("trade_count_24h").ok();

                let open = open.unwrap_or(rust_decimal::Decimal::ZERO);
                let last = last.unwrap_or(rust_decimal::Decimal::ZERO);
                let high = high.unwrap_or(rust_decimal::Decimal::ZERO);
                let low = low.unwrap_or(rust_decimal::Decimal::ZERO);
                let volume = volume.unwrap_or(rust_decimal::Decimal::ZERO);
                let count = count.unwrap_or(0);

                let change = last - open;
                let pct = if open.is_zero() {
                    rust_decimal::Decimal::ZERO
                } else {
                    (change / open) * rust_decimal::Decimal::ONE_HUNDRED
                };

                GetTickerResponse {
                    symbol,
                    last_price: last.to_string(),
                    price_change_24h: change.to_string(),
                    price_change_pct_24h: format!("{:.4}", pct),
                    high_24h: high.to_string(),
                    low_24h: low.to_string(),
                    volume_24h: volume.to_string(),
                    open_24h: open.to_string(),
                    trade_count_24h: count,
                }
            }
        };

        Ok(Response::new(response))
    }

    async fn get_candles(
        &self,
        request: Request<GetCandlesRequest>,
    ) -> Result<Response<GetCandlesResponse>, Status> {
        let req = request.into_inner();

        let view_name = match req.resolution.as_str() {
            "1m" => "candles_1m",
            "5m" => "candles_5m",
            "1h" => "candles_1h",
            _ => {
                return Err(Status::invalid_argument(
                    "Invalid resolution. Must be '1m', '5m', or '1h'.",
                ));
            }
        };

        let query_str = format!(
            r#"
            SELECT bucket, open, high, low, close, volume
            FROM {}
            WHERE symbol = $1
            ORDER BY bucket DESC
            LIMIT $2
            "#,
            view_name
        );

        let rows = sqlx::query(&query_str)
            .bind(&req.symbol)
            .bind(req.limit)
            .fetch_all(&self.db_pool)
            .await
            .map_err(|e| Status::internal(format!("Database query error: {:?}", e)))?;

        let mut candles = Vec::new();
        for row in rows {
            let bucket: chrono::DateTime<chrono::Utc> = row
                .try_get("bucket")
                .map_err(|e| Status::internal(e.to_string()))?;
            let open: rust_decimal::Decimal = row
                .try_get("open")
                .map_err(|e| Status::internal(e.to_string()))?;
            let high: rust_decimal::Decimal = row
                .try_get("high")
                .map_err(|e| Status::internal(e.to_string()))?;
            let low: rust_decimal::Decimal = row
                .try_get("low")
                .map_err(|e| Status::internal(e.to_string()))?;
            let close: rust_decimal::Decimal = row
                .try_get("close")
                .map_err(|e| Status::internal(e.to_string()))?;
            let volume: rust_decimal::Decimal = row
                .try_get("volume")
                .map_err(|e| Status::internal(e.to_string()))?;

            candles.push(CandleInfo {
                timestamp: bucket.timestamp(),
                open: open.to_string(),
                high: high.to_string(),
                low: low.to_string(),
                close: close.to_string(),
                volume: volume.to_string(),
            });
        }

        Ok(Response::new(GetCandlesResponse { candles }))
    }
}
