use actix_web::{HttpResponse, ResponseError, http::StatusCode};
use errors::app_error::ServiceError;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    Service(#[from] ServiceError),
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::Service(ServiceError::InvalidTickSize) => StatusCode::BAD_REQUEST,
            ApiError::Service(ServiceError::InvalidLotSize) => StatusCode::BAD_REQUEST,
            ApiError::Service(ServiceError::InvalidLeverage) => StatusCode::BAD_REQUEST,
            ApiError::Service(ServiceError::Repository(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::Service(ServiceError::MarketAlreadyExists) => StatusCode::CONFLICT,
            ApiError::Service(ServiceError::InvalidStatus) => StatusCode::BAD_REQUEST,
            ApiError::Service(ServiceError::MarketNotFound) => StatusCode::NOT_FOUND,
            ApiError::Service(ServiceError::InsufficientBalance) => StatusCode::BAD_REQUEST,
            ApiError::Service(ServiceError::NotFound) => StatusCode::NOT_FOUND,
            ApiError::Service(ServiceError::Validation(_)) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let body = ErrorResponse {
            code: self.code(),
            message: self.to_string(),
        };
        HttpResponse::build(self.status_code()).json(body)
    }
}

impl ApiError {
    fn code(&self) -> String {
        match self {
            ApiError::Service(ServiceError::InvalidTickSize) => "INVALID_TICK_SIZE".into(),
            ApiError::Service(ServiceError::InvalidLotSize) => "INVALID_LOT_SIZE".into(),
            ApiError::Service(ServiceError::InvalidLeverage) => "INVALID_LEVERAGE".into(),
            ApiError::Service(ServiceError::Repository(_)) => "DATABASE_ERROR".into(),
            ApiError::Service(ServiceError::MarketAlreadyExists) => "MARKET_ALREADY_EXISTS".into(),
            ApiError::Service(ServiceError::InvalidStatus) => "INVALID_STATUS".into(),
            ApiError::Service(ServiceError::MarketNotFound) => "MARKET_NOT_FOUND".into(),
            ApiError::Service(ServiceError::InsufficientBalance) => "INSUFFICIENT_BALANCE".into(),
            ApiError::Service(ServiceError::NotFound) => "NOT_FOUND".into(),
            ApiError::Service(ServiceError::Validation(_)) => "VALIDATION_ERROR".into(),
        }
    }
}
