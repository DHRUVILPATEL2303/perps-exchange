use std::collections::HashSet;
use futures_util::StreamExt;
use redis::AsyncCommands;
use tokio::sync::mpsc;

pub async fn handle_connection_pubsub(
    mut channel_sub_rx: mpsc::Receiver<Vec<String>>,
    text_sender: mpsc::Sender<String>,
    redis_client: redis::Client,
) {
    let mut pubsub = match redis_client.get_async_pubsub().await {
        Ok(ps) => ps,
        Err(e) => {
            tracing::error!("Failed to get Redis pubsub: {:?}", e);
            return;
        }
    };

    let mut subscribed_channels = HashSet::new();

    loop {
        let mut pubsub_stream = pubsub.on_message();

        tokio::select! {
            // match the channel result explicitly
            res = channel_sub_rx.recv() => {
                match res {
                    Some(channels) => {
                        drop(pubsub_stream);
                        for channel in channels {
                            if subscribed_channels.insert(channel.clone()) {
                                if let Err(e) = pubsub.subscribe(&channel).await {
                                    tracing::error!("Failed to subscribe to Redis channel {}: {:?}", channel, e);
                                    subscribed_channels.remove(&channel);
                                }
                            }
                        }
                    }
                    None => {
                        break;
                    }
                }
            }
            Some(msg) = pubsub_stream.next() => {
                if let Ok(payload) = msg.get_payload::<String>() {
                    if text_sender.send(payload).await.is_err() {
                        break;
                    }
                }
            }
        }
    }
}
