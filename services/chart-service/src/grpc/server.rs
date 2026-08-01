use proto::chart::{
    CandleInfo, GetCandlesRequest, GetCandlesResponse,
    chart_service_server::ChartService as GrpcChartService,
};
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};

pub struct ChartGrpcService {
    pub db_pool: PgPool,
}

#[tonic::async_trait]
impl GrpcChartService for ChartGrpcService {
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
