use actix_web::{web::{Data, Payload}, HttpRequest, HttpResponse, Error};
use tokio_stream::StreamExt;
use crate::state::AppState;

pub async fn ws_index(
    state: Data<AppState>,
    req: HttpRequest,
    stream: Payload,
) -> Result<HttpResponse, Error> {
    let (res, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;

    {
        let mut lock = state.ws_sessions.lock().await;
        lock.push(session.clone());
    }

    tokio::task::spawn_local(async move {
        while let Some(Ok(msg)) = msg_stream.next().await {
            match msg {
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
