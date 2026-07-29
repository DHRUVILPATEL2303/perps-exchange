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