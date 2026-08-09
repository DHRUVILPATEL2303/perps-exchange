use actix_web::{web::{Data, Payload}, HttpRequest, HttpResponse, Error};
use tokio_stream::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc;
use crate::state::AppState;
use crate::presentation::handlers::ws_router::handle_connection_pubsub;

#[derive(Deserialize)]
struct WsAction {
    action: String,
    channels: Vec<String>,
}

#[derive(Deserialize)]
struct WsQuery {
    token: Option<String>,
}

pub async fn ws_index(
    state: Data<AppState>,
    req: HttpRequest,
    stream: Payload,
) -> Result<HttpResponse, Error> {
    let query = match actix_web::web::Query::<WsQuery>::from_query(req.query_string()) {
        Ok(q) => q.into_inner(),
        Err(_) => return Ok(HttpResponse::Unauthorized().body("Invalid query parameters")),
    };

    let token = match query.token {
        Some(t) => t,
        None => return Ok(HttpResponse::Unauthorized().body("Missing token query parameter")),
    };

    let jwt_secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| "default_secret_key_change_me_in_production".to_string());
    if jsonwebtoken::decode::<crate::presentation::handlers::auth_handler::Claims>(
        &token,
        &jsonwebtoken::DecodingKey::from_secret(jwt_secret.as_bytes()),
        &jsonwebtoken::Validation::default(),
    ).is_err() {
        return Ok(HttpResponse::Unauthorized().body("Invalid or expired token"));
    }

    let (res, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;
    let redis_client = state.redis_client.clone();

    let (channel_sub_tx, channel_sub_rx) = mpsc::channel::<Vec<String>>(100);
    let (text_sender_tx, mut text_sender_rx) = mpsc::channel::<String>(100);

    tokio::spawn(async move {
        handle_connection_pubsub(channel_sub_rx, text_sender_tx, redis_client).await;
    });

    let mut ws_session_writer = session.clone();
    tokio::spawn(async move {
        while let Some(msg) = text_sender_rx.recv().await {
            if ws_session_writer.text(msg).await.is_err() {
                break;
            }
        }
    });

    tokio::task::spawn_local(async move {
        while let Some(Ok(msg)) = msg_stream.next().await {
            match msg {
                actix_ws::Message::Text(text) => {
                    if let Ok(sub_action) = serde_json::from_str::<WsAction>(&text) {
                        if sub_action.action == "subscribe" {
                            let _ = channel_sub_tx.send(sub_action.channels).await;
                        }
                    }
                }
                actix_ws::Message::Ping(bytes) => {
                    let _ = session.pong(&bytes).await;
                }
                actix_ws::Message::Close(reason) => {
                    let _ = session.close(reason).await;
                    break;
                }
                _ => {}
            }
        }
    });

    Ok(res)
}
