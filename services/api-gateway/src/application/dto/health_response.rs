use serde::Serialize;


#[derive(Debug,Clone,Serialize)]
pub struct HealthResponse {
    pub status : String,
    pub service : String 
}