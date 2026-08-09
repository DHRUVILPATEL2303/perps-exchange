use std::time::Duration;
use anyhow::Result;
use wtransport::{ServerConfig, Identity};
use wtransport::tls::Sha256DigestFmt;
use tokio::sync::mpsc;
use serde::Deserialize;
use jsonwebtoken;
use crate::presentation::handlers::ws_router::handle_connection_pubsub;

#[derive(Deserialize)]
struct WtAction {
    action: String,
    channels: Vec<String>,
}

pub async fn run_webtransport_server(redis_client: redis::Client) -> Result<()> {
    let sans: &[&str] = &["localhost", "127.0.0.1", "::1"];
    let identity = Identity::self_signed(sans)?;
    
    let fingerprint = identity.certificate_chain().as_slice()[0]
        .hash()
        .fmt(Sha256DigestFmt::BytesArray);
    
    println!("\n==================================================================");
    println!("WebTransport Server Certificate Fingerprint (SHA-256):");
    println!("{}", fingerprint);
    println!("==================================================================\n");
    let config = ServerConfig::builder()
        .with_bind_address("0.0.0.0:4433".parse().unwrap())
        .with_identity(&identity.clone_identity())
        .keep_alive_interval(Some(Duration::from_secs(3))) 
        .build();


    let server = wtransport::Endpoint::server(config)?;

    loop {
        let incoming = server.accept().await;

        let redis_clone = redis_client.clone();
        tokio::spawn(async move {
    
            let session_request = match incoming.await {
                Ok(req) => req,
                Err(e) => {
                    tracing::error!("Failed to accept incoming WT session: {:?}", e);
                    return;
                }
            };

            let path = session_request.path();
            let mut token = None;
            if let Some(pos) = path.find("token=") {
                let token_part = &path[pos + 6..];
                if let Some(end) = token_part.find('&') {
                    token = Some(&token_part[..end]);
                } else {
                    token = Some(token_part);
                }
            }

            let token = match token {
                Some(t) => t,
                None => {
                    tracing::error!("Missing token query parameter in WebTransport connection request");
                    return;
                }
            };

            let jwt_secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| "default_secret_key_change_me_in_production".to_string());
            if jsonwebtoken::decode::<crate::presentation::handlers::auth_handler::Claims>(
                token,
                &jsonwebtoken::DecodingKey::from_secret(jwt_secret.as_bytes()),
                &jsonwebtoken::Validation::default(),
            ).is_err() {
                tracing::error!("Invalid or expired token in WebTransport connection request");
                return;
            }

            let session = match session_request.accept().await {
                Ok(sess) => sess,
                Err(e) => {
                    tracing::error!("Failed to establish WT session: {:?}", e);
                    return;
                }
            };

            let (mut send_stream, mut recv_stream) = match session.accept_bi().await {
                Ok(stream) => stream,
                Err(_) => return,
            };

            let (channel_sub_tx, channel_sub_rx) = mpsc::channel::<Vec<String>>(100);
            let (text_sender_tx, mut text_sender_rx) = mpsc::channel::<String>(100);

            tokio::spawn(async move {
                handle_connection_pubsub(channel_sub_rx, text_sender_tx, redis_clone).await;
            });

            tokio::spawn(async move {
                while let Some(msg) = text_sender_rx.recv().await {
                    if send_stream.write_all(msg.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });

            let mut buf = vec![0u8; 1024];
            loop {
                match recv_stream.read(&mut buf).await {
                    Ok(Some(n)) => {
                        if let Ok(text) = std::str::from_utf8(&buf[..n]) {
                            if let Ok(sub_action) = serde_json::from_str::<WtAction>(text) {
                                if sub_action.action == "subscribe" {
                                    let _ = channel_sub_tx.send(sub_action.channels).await;
                                }
                            }
                        }
                    }
                    _ => break,
                }
            }
        });
    }
}
