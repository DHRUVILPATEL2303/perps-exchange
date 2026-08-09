use actix_web::web::{Data, Json};
use actix_web::HttpResponse;
use crate::state::AppState;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use redis::AsyncCommands;
use jsonwebtoken::{encode, Header, EncodingKey};

#[derive(Deserialize)]
pub struct ChallengeRequest {
    pub public_key: String,
}

#[derive(Serialize)]
pub struct ChallengeResponse {
    pub nonce: String,
}

#[derive(Deserialize)]
pub struct LoginRequest {
    pub public_key: String,
    pub signature: String,
    pub nonce: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub user_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub pubkey: String,
    pub exp: usize,
}

pub async fn get_challenge(
    state: Data<AppState>,
    body: Json<ChallengeRequest>,
) -> HttpResponse {
    let req = body.into_inner();
    let nonce = Uuid::new_v4().to_string();
    let redis_key = format!("challenge:{}", req.public_key);

    let mut redis_conn = match state.redis_client.get_multiplexed_async_connection().await {
        Ok(conn) => conn,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let _: Result<(), _> = redis_conn.set_ex(&redis_key, &nonce, 60).await;

    HttpResponse::Ok().json(ChallengeResponse { nonce })
}

pub async fn login(
    state: Data<AppState>,
    body: Json<LoginRequest>,
) -> HttpResponse {
    let req = body.into_inner();
    let redis_key = format!("challenge:{}", req.public_key);

    let mut redis_conn = match state.redis_client.get_multiplexed_async_connection().await {
        Ok(conn) => conn,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let stored_nonce: Result<String, _> = redis_conn.get(&redis_key).await;
    let stored_nonce = match stored_nonce {
        Ok(n) => n,
        Err(_) => return HttpResponse::BadRequest().body("Challenge expired or invalid"),
    };

    if stored_nonce != req.nonce {
        return HttpResponse::BadRequest().body("Challenge mismatch");
    }

    let _: Result<(), _> = redis_conn.del(&redis_key).await;

    let message = format!("Sign-in to Perpetuals Exchange: {}", req.nonce);

    let pubkey_bytes = match bs58::decode(&req.public_key).into_vec() {
        Ok(bytes) => bytes,
        Err(_) => return HttpResponse::BadRequest().body("Invalid public key encoding"),
    };

    let sig_bytes = match bs58::decode(&req.signature).into_vec() {
        Ok(bytes) => bytes,
        Err(_) => return HttpResponse::BadRequest().body("Invalid signature encoding"),
    };

    if pubkey_bytes.len() != 32 || sig_bytes.len() != 64 {
        return HttpResponse::BadRequest().body("Invalid key or signature length");
    }

    let mut pubkey_array = [0u8; 32];
    pubkey_array.copy_from_slice(&pubkey_bytes);

    let mut sig_array = [0u8; 64];
    sig_array.copy_from_slice(&sig_bytes);

    let verifying_key = match VerifyingKey::from_bytes(&pubkey_array) {
        Ok(key) => key,
        Err(_) => return HttpResponse::BadRequest().body("Invalid public key"),
    };

    let signature = Signature::from_bytes(&sig_array);

    if verifying_key.verify(message.as_bytes(), &signature).is_err() {
        return HttpResponse::Unauthorized().body("Signature verification failed");
    }

    let user_id = Uuid::new_v5(&Uuid::NAMESPACE_URL, req.public_key.as_bytes()).to_string();

    let jwt_secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| "default_secret_key_change_me_in_production".to_string());
    let claims = Claims {
        sub: user_id.clone(),
        pubkey: req.public_key,
        exp: (chrono::Utc::now() + chrono::Duration::hours(24)).timestamp() as usize,
    };

    let token = match encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(jwt_secret.as_bytes()),
    ) {
        Ok(t) => t,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    HttpResponse::Ok().json(LoginResponse { token, user_id })
}

pub struct AuthenticatedUser {
    pub user_id: String,
    pub pubkey: String,
}

impl actix_web::FromRequest for AuthenticatedUser {
    type Error = actix_web::Error;
    type Future = futures_util::future::Ready<Result<Self, Self::Error>>;

    fn from_request(req: &actix_web::HttpRequest, _payload: &mut actix_web::dev::Payload) -> Self::Future {
        let auth_header = match req.headers().get("Authorization") {
            Some(val) => match val.to_str() {
                Ok(s) => s,
                Err(_) => return futures_util::future::ready(Err(actix_web::error::ErrorUnauthorized("Invalid auth header encoding"))),
            },
            None => return futures_util::future::ready(Err(actix_web::error::ErrorUnauthorized("Missing Authorization header"))),
        };

        if !auth_header.starts_with("Bearer ") {
            return futures_util::future::ready(Err(actix_web::error::ErrorUnauthorized("Auth header must start with Bearer ")));
        }

        let token = &auth_header[7..];
        let jwt_secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| "default_secret_key_change_me_in_production".to_string());

        match jsonwebtoken::decode::<Claims>(
            token,
            &jsonwebtoken::DecodingKey::from_secret(jwt_secret.as_bytes()),
            &jsonwebtoken::Validation::default(),
        ) {
            Ok(token_data) => futures_util::future::ready(Ok(AuthenticatedUser {
                user_id: token_data.claims.sub,
                pubkey: token_data.claims.pubkey,
            })),
            Err(_) => futures_util::future::ready(Err(actix_web::error::ErrorUnauthorized("Invalid or expired token"))),
        }
    }
}
