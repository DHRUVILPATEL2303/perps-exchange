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
    let claims = match jsonwebtoken::decode::<crate::presentation::handlers::auth_handler::Claims>(
        &token,
        &jsonwebtoken::DecodingKey::from_secret(jwt_secret.as_bytes()),
        &jsonwebtoken::Validation::default(),
    ) {
        Ok(data) => data.claims,
        Err(_) => return Ok(HttpResponse::Unauthorized().body("Invalid or expired token")),
    };

    let user_id = claims.sub.clone();
    let (res, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;
    let redis_client = state.redis_client.clone();

    let session_id = uuid::Uuid::new_v4();
    let ws_sessions_clone = state.ws_sessions.clone();
    let user_id_clone = user_id.clone();
    let session_clone = session.clone();

    tokio::spawn(async move {
        let mut sessions = ws_sessions_clone.lock().await;
        sessions.entry(user_id_clone).or_default().push((session_id, session_clone));
    });

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

    let ws_sessions_for_cleanup = state.ws_sessions.clone();
    let user_id_for_cleanup = user_id.clone();

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

        let mut sessions = ws_sessions_for_cleanup.lock().await;
        if let Some(list) = sessions.get_mut(&user_id_for_cleanup) {
            list.retain(|(sid, _)| *sid != session_id);
            if list.is_empty() {
                sessions.remove(&user_id_for_cleanup);
            }
        }
    });

    Ok(res)
}
