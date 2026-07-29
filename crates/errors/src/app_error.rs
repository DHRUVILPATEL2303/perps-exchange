use thiserror::Error;
use actix_web::{HttpResponse, ResponseError};

#[derive(Debug,Error)]
pub enum AppError {

    #[error("Internal Server Error")]
    Internal ,

    #[error("Not Found")]
    NotFound,

    #[error("Bad Request")]
    BadRequest,
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("Internal Server Error")]
    InternalServerError,

    #[error("Not Found")]
    NotFound,

    #[error("Bad Request")]
    BadRequest,
    
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
}


impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        match self {
            AppError::BadRequest => {
                HttpResponse::BadRequest().finish()
            }
            AppError::Internal => {
                HttpResponse::InternalServerError().finish()
            }

            AppError::NotFound => {
                HttpResponse::NotFound().finish()
            }
            
        }
        
    }
}