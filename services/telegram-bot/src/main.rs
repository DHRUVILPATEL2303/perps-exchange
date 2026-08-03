use futures_util::StreamExt;
use rdkafka::Message as KafkaMessage;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use teloxide::prelude::*;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup, Message, WebAppInfo};
use teloxide::utils::command::BotCommands;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct Alert {
    chat_id: ChatId,
    target_price: Decimal,
    is_above: bool,
}

#[derive(Deserialize, Debug)]
struct PriceTick {
    symbol: String,
    mark_price: Decimal,
}

#[derive(BotCommands, Clone)]
#[command(
    rename_rule = "lowercase",
    description = "These commands are supported:"
)]
enum Command {
    #[command(description = "display this text.")]
    Help,
    #[command(description = "start the bot and launch the Mini App.")]
    Start,
    #[command(description = "set a price alert: /alert <symbol> <direction (> or <)> <price>.")]
    Alert(String),
    #[command(description = "list your active alerts.")]
    List,
    #[command(description = "clear all your alerts.")]
    Clear,
}

type AlertMap = Arc<Mutex<HashMap<String, Vec<Alert>>>>;

#[tokio::main]
async fn main() {
    telemetry::logging::init();
    tracing::info!("Starting Telegram Bot Service...");

    let bot = Bot::from_env();
    let alerts: AlertMap = Arc::new(Mutex::new(HashMap::new()));

    // Spawn the Kafka Price Consumer for Alerts
    let alerts_clone = alerts.clone();
    let bot_clone = bot.clone();
    tokio::spawn(async move {
        if let Err(e) = run_kafka_price_consumer(bot_clone, alerts_clone).await {
            tracing::error!("Kafka price consumer crashed: {:?}", e);
        }
    });

    Command::repl(bot, move |bot, msg, cmd| {
        let alerts = alerts.clone();
        async move { handle_command(bot, msg, cmd, alerts).await }
    })
    .await;
}

async fn handle_command(
    bot: Bot,
    msg: Message,
    cmd: Command,
    alerts: AlertMap,
) -> ResponseResult<()> {
    match cmd {
        Command::Help => {
            bot.send_message(msg.chat.id, Command::descriptions().to_string())
                .await?;
        }
        Command::Start => {
            let app_url = std::env::var("TELEGRAM_MINI_APP_URL").unwrap_or_else(|_| {
                "https://dhruvilpatel.github.io/perps-tma-placeholder".to_string()
            });

            let keyboard = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::web_app(
                "Open Perps App",
                WebAppInfo {
                    url: app_url.parse().unwrap(),
                },
            )]]);

            bot.send_message(
                msg.chat.id,
                "Welcome to the Perps Exchange! 📈\n\nClick the button below to launch the Trading Mini App and manage your order books, positions, and trades natively within Telegram."
            )
            .reply_markup(keyboard)
            .await?;
        }
        Command::Alert(args) => {
            // Expected args: "BTCUSDT > 65000" or "BTCUSDT < 63000"
            let parts: Vec<&str> = args.split_whitespace().collect();
            if parts.len() != 3 {
                bot.send_message(msg.chat.id, "Invalid format! Use: /alert <symbol> < > or < > <price>\nExample: /alert BTCUSDT > 65000").await?;
                return Ok(());
            }

            let symbol = parts[0].to_uppercase();
            let direction = parts[1];
            let price_str = parts[2];

            let target_price = match Decimal::from_str(price_str) {
                Ok(p) => p,
                Err(_) => {
                    bot.send_message(msg.chat.id, "Invalid price decimal!")
                        .await?;
                    return Ok(());
                }
            };

            let is_above = match direction {
                ">" => true,
                "<" => false,
                _ => {
                    bot.send_message(msg.chat.id, "Invalid direction! Use > or <")
                        .await?;
                    return Ok(());
                }
            };

            let new_alert = Alert {
                chat_id: msg.chat.id,
                target_price,
                is_above,
            };

            {
                let mut map = alerts.lock().await;
                map.entry(symbol.clone()).or_default().push(new_alert);
            }

            bot.send_message(
                msg.chat.id,
                format!(
                    "Alert set! We will notify you when {} goes {} {}",
                    symbol, direction, target_price
                ),
            )
            .await?;
        }
        Command::List => {
            let map = alerts.lock().await;
            let mut list_msg = "🔔 Your Active Alerts:\n".to_string();
            let mut count = 0;

            for (symbol, list) in map.iter() {
                for alert in list.iter() {
                    if alert.chat_id == msg.chat.id {
                        let dir = if alert.is_above { ">" } else { "<" };
                        list_msg
                            .push_str(&format!("• {} {} {}\n", symbol, dir, alert.target_price));
                        count += 1;
                    }
                }
            }

            if count == 0 {
                bot.send_message(msg.chat.id, "You have no active alerts.")
                    .await?;
            } else {
                bot.send_message(msg.chat.id, list_msg).await?;
            }
        }
        Command::Clear => {
            let mut map = alerts.lock().await;
            for list in map.values_mut() {
                list.retain(|alert| alert.chat_id != msg.chat.id);
            }
            bot.send_message(msg.chat.id, "All your alerts have been cleared.")
                .await?;
        }
    }
    Ok(())
}

async fn run_kafka_price_consumer(bot: Bot, alerts: AlertMap) -> anyhow::Result<()> {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "telegram-bot-alerts-group")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .create()?;

    consumer.subscribe(&["price-feed"])?;
    tracing::info!("Telegram Bot alerts consumer subscribed to price-feed topic.");

    let mut stream = consumer.stream();
    while let Some(msg_res) = stream.next().await {
        if let Ok(msg) = msg_res {
            if let Some(payload) = KafkaMessage::payload(&msg) {
                if let Ok(tick) = serde_json::from_slice::<PriceTick>(payload) {
                    let current_price = tick.mark_price;
                    let mut alerts_to_trigger = Vec::new();

                    // Check if any alerts matched
                    {
                        let mut map = alerts.lock().await;
                        if let Some(list) = map.get_mut(&tick.symbol) {
                            let mut i = 0;
                            while i < list.len() {
                                let alert = &list[i];
                                let triggered = if alert.is_above {
                                    current_price >= alert.target_price
                                } else {
                                    current_price <= alert.target_price
                                };

                                if triggered {
                                    alerts_to_trigger.push(list.remove(i));
                                } else {
                                    i += 1;
                                }
                            }
                        }
                    }

                    // Send alert notifications
                    for alert in alerts_to_trigger {
                        let direction = if alert.is_above {
                            "rose above"
                        } else {
                            "fell below"
                        };
                        let _ = bot.send_message(
                            alert.chat_id,
                            format!("🔔 **PRICE ALERT**\n\n{} has {} your target of **{}**! Current price: **{}**",
                                tick.symbol, direction, alert.target_price, current_price)
                        ).await;
                    }
                }
            }
        }
    }

    Ok(())
}
