use actix_web::{HttpResponse, ResponseError, http::StatusCode};
use thiserror::Error;
use serde::Serialize;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Internal Server Error")]
    Internal,

    #[error("Not Found")]
    NotFound,

    #[error("Bad Request")]
    BadRequest,
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    Service(#[from] ServiceError),
}



#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Entity not found")]
    NotFound,

    #[error("Duplicate entity")]
    Duplicate,

    #[error("Unknown repository error")]
    Unknown,

 
}

#[derive(Debug, Error)]
pub enum ServiceError {
  
     #[error(transparent)]
    Repository(#[from] RepositoryError),

    #[error("Invalid tick size")]
    InvalidTickSize,

    #[error("Invalid lot size")]
    InvalidLotSize,

    #[error("Invalid leverage")]
    InvalidLeverage,

    #[error("Market already exists")]
    MarketAlreadyExists,

    #[error("Invalid status")]
    InvalidStatus,

    #[error("Market not found")]
    MarketNotFound,

    #[error("Insufficient balance")]
    InsufficientBalance,

    #[error("Not found")]
    NotFound,

    #[error("Validation error: {0}")]
    Validation(String),
}

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        match self {
            AppError::BadRequest => HttpResponse::BadRequest().finish(),
            AppError::Internal => HttpResponse::InternalServerError().finish(),

            AppError::NotFound => HttpResponse::NotFound().finish(),
        }
    }
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
    pub fn code(&self) -> String {
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
