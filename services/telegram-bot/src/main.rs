use futures_util::StreamExt;
use rdkafka::Message as KafkaMessage;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use redis::AsyncCommands;
use rust_decimal::Decimal;
use serde::Deserialize;
use sqlx::Row;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use teloxide::prelude::*;
use teloxide::types::{ChatId, InlineKeyboardButton, InlineKeyboardMarkup, Message, WebAppInfo};
use teloxide::utils::command::BotCommands;
use tokio::sync::Mutex;
use uuid::{self, Uuid};

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
    #[command(description = "link your exchange UUID: /register <user_id>.")]
    Register(String),
    #[command(description = "get instructions on linking your exchange account.")]
    Link,
    #[command(description = "unlink your exchange account.")]
    Unlink,
    #[command(description = "get support contact details.")]
    Support,
    #[command(description = "check your exchange balances.")]
    Balance,
    #[command(description = "view your active perp positions.")]
    Positions,
    #[command(description = "view your trade execution history.")]
    Trades,
    #[command(description = "view your account transaction history (deposits/withdrawals).")]
    History,
}

type AlertMap = Arc<Mutex<HashMap<String, Vec<Alert>>>>;

#[tokio::main]
async fn main() {
    telemetry::logging::init();
    tracing::info!("Starting Telegram Bot Service...");

    let bot = Bot::from_env();
    let alerts: AlertMap = Arc::new(Mutex::new(HashMap::new()));

    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5432/perps_accounts".to_string()
    });

    let db_pool = sqlx::PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to database in telegram-bot");

    let redis_host = std::env::var("REDIS__HOST").unwrap_or_else(|_| "localhost".to_string());
    let redis_port = std::env::var("REDIS__PORT").unwrap_or_else(|_| "6379".to_string());
    let redis_url = format!("redis://{}:{}", redis_host, redis_port);
    let redis_client =
        redis::Client::open(redis_url).expect("Failed to open Redis client in telegram-bot");

    let alerts_clone = alerts.clone();
    let bot_clone = bot.clone();
    tokio::spawn(async move {
        if let Err(e) = run_kafka_price_consumer(bot_clone, alerts_clone).await {
            tracing::error!("Kafka price consumer crashed: {:?}", e);
        }
    });

    let db_pool_clone = db_pool.clone();
    let bot_clone2 = bot.clone();
    tokio::spawn(async move {
        if let Err(e) = run_kafka_notification_consumer(bot_clone2, db_pool_clone).await {
            tracing::error!("Kafka notification consumer crashed: {:?}", e);
        }
    });

    let db_pool_clone2 = db_pool.clone();
    let redis_client_clone = redis_client.clone();

    let handler = Update::filter_message().branch(
        dptree::entry()
            .branch(
                dptree::filter(|msg: Message| {
                    msg.text().map_or(false, |t| t.starts_with("/start"))
                })
                .endpoint(move |bot: Bot, msg: Message| {
                    let db = db_pool_clone2.clone();
                    let redis = redis_client_clone.clone();
                    async move { handle_start_command(bot, msg, db, redis).await }
                }),
            )
            .branch(dptree::entry().filter_command::<Command>().endpoint(
                move |bot: Bot, msg: Message, cmd: Command| {
                    let alerts = alerts.clone();
                    let db = db_pool.clone();
                    async move { handle_command(bot, msg, cmd, alerts, db).await }
                },
            )),
    );

    let bot_commands = vec![
        teloxide::types::BotCommand::new("help", "Display help message"),
        teloxide::types::BotCommand::new("start", "Launch the Mini App"),
        teloxide::types::BotCommand::new(
            "alert",
            "Set a price alert: /alert <symbol> <direction> <price>",
        ),
        teloxide::types::BotCommand::new("list", "List active price alerts"),
        teloxide::types::BotCommand::new("clear", "Clear all price alerts"),
        teloxide::types::BotCommand::new("link", "Link your exchange account"),
        teloxide::types::BotCommand::new("unlink", "Unlink your exchange account"),
        teloxide::types::BotCommand::new("support", "Contact support"),
        teloxide::types::BotCommand::new("balance", "Check your balances"),
        teloxide::types::BotCommand::new("positions", "View active perp positions"),
        teloxide::types::BotCommand::new("trades", "View trade history"),
        teloxide::types::BotCommand::new("history", "View deposit/withdrawal history"),
    ];
    let _ = bot.set_my_commands(bot_commands).await;

    Dispatcher::builder(bot, handler)
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}

async fn handle_command(
    bot: Bot,
    msg: Message,
    cmd: Command,
    alerts: AlertMap,
    db: sqlx::Pool<sqlx::Postgres>,
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
        Command::Register(user_id_str) => {
            let user_id = match Uuid::parse_str(&user_id_str) {
                Ok(u) => u,
                Err(_) => {
                    bot.send_message(msg.chat.id, "Invalid UUID format! Example: /register 11111111-2222-3333-4444-555555555555").await?;
                    return Ok(());
                }
            };

            let query_res = sqlx::query(
                r#"
                INSERT INTO telegram_user_mappings (user_id, telegram_chat_id, created_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (user_id) DO UPDATE SET telegram_chat_id = EXCLUDED.telegram_chat_id
                "#,
            )
            .bind(user_id)
            .bind(msg.chat.id.0)
            .execute(&db)
            .await;

            match query_res {
                Ok(_) => {
                    bot.send_message(msg.chat.id, "Account successfully linked! You will now receive push notifications on Telegram for order fills when you are offline.").await?;
                }
                Err(e) => {
                    bot.send_message(msg.chat.id, format!("Failed to register: {:?}", e))
                        .await?;
                }
            }
        }
        Command::Alert(args) => {
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
        Command::Link => {
            bot.send_message(
                msg.chat.id,
                "🔗 **How to link your exchange account**:\n\n1. Sign in to the web trading platform.\n2. Go to settings -> Telegram Bot linkage.\n3. Click the secure deep link to verify and automatically connect your Telegram chat."
            )
            .await?;
        }
        Command::Unlink => {
            let query_res =
                sqlx::query("DELETE FROM telegram_user_mappings WHERE telegram_chat_id = $1")
                    .bind(msg.chat.id.0)
                    .execute(&db)
                    .await;

            match query_res {
                Ok(_) => {
                    bot.send_message(msg.chat.id, "Your Telegram account has been successfully unlinked from the exchange user UUID.").await?;
                }
                Err(e) => {
                    bot.send_message(msg.chat.id, format!("Failed to unlink: {:?}", e))
                        .await?;
                }
            }
        }
        Command::Support => {
            bot.send_message(
                msg.chat.id,
                "⚙️ **Support & Help**:\n\nIf you have any questions or issues, feel free to reach out:\n\n📧 Email: support@perpsexchange.io\n💬 Telegram Support: @perpsexchange_support"
            )
            .await?;
        }
        Command::Balance => {
            let user_id_opt = match sqlx::query(
                "SELECT user_id FROM telegram_user_mappings WHERE telegram_chat_id = $1",
            )
            .bind(msg.chat.id.0)
            .fetch_optional(&db)
            .await
            {
                Ok(Some(row)) => {
                    let uid: Uuid = row.get(0);
                    Some(uid.to_string())
                }
                _ => None,
            };

            let user_id = match user_id_opt {
                Some(uid) => uid,
                None => {
                    bot.send_message(msg.chat.id, "❌ Your Telegram account is not linked to any exchange account. Use /link to see instructions on how to link your wallet.").await?;
                    return Ok(());
                }
            };

            let account_service_url = std::env::var("ACCOUNT_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:50053".to_string());

            let mut client =
                match proto::account::account_service_client::AccountServiceClient::connect(
                    account_service_url,
                )
                .await
                {
                    Ok(c) => c,
                    Err(_) => {
                        bot.send_message(
                            msg.chat.id,
                            "❌ Failed to connect to Account Service. Please try again later.",
                        )
                        .await?;
                        return Ok(());
                    }
                };

            let usdc_res = client
                .get_balance(proto::account::GetBalanceRequest {
                    user_id: user_id.clone(),
                    asset: "USDC".to_string(),
                })
                .await;

            let usdt_res = client
                .get_balance(proto::account::GetBalanceRequest {
                    user_id: user_id.clone(),
                    asset: "USDT".to_string(),
                })
                .await;

            fn fmt_amount(s: &str) -> String {
                rust_decimal::Decimal::from_str(s)
                    .map(|d| format!("{:.4}", d))
                    .unwrap_or_else(|_| s.to_string())
            }

            let mut response_text = String::from("💰 *Your Exchange Balances*\n\n");

            match usdc_res {
                Ok(r) => {
                    let res = r.into_inner();
                    let avail = fmt_amount(&res.available_balance);
                    let locked = fmt_amount(&res.locked_balance);
                    let total = rust_decimal::Decimal::from_str(&res.available_balance)
                        .unwrap_or_default()
                        + rust_decimal::Decimal::from_str(&res.locked_balance).unwrap_or_default();
                    response_text.push_str(&format!(
                        "*USDC*\n`Total:     {:>12}\nAvailable: {:>12}\nLocked:    {:>12}`\n\n",
                        format!("{:.4}", total),
                        avail,
                        locked
                    ));
                }
                Err(_) => response_text.push_str("*USDC*: \u{274c} Unavailable\n\n"),
            }

            match usdt_res {
                Ok(r) => {
                    let res = r.into_inner();
                    let avail = fmt_amount(&res.available_balance);
                    let locked = fmt_amount(&res.locked_balance);
                    let total = rust_decimal::Decimal::from_str(&res.available_balance)
                        .unwrap_or_default()
                        + rust_decimal::Decimal::from_str(&res.locked_balance).unwrap_or_default();
                    response_text.push_str(&format!(
                        "*USDT*\n`Total:     {:>12}\nAvailable: {:>12}\nLocked:    {:>12}`\n\n",
                        format!("{:.4}", total),
                        avail,
                        locked
                    ));
                }
                Err(_) => response_text.push_str("*USDT*: \u{274c} Unavailable\n\n"),
            }

            bot.send_message(msg.chat.id, response_text)
                .parse_mode(teloxide::types::ParseMode::MarkdownV2)
                .await?;
        }
        Command::Positions => {
            let user_id_opt = match sqlx::query(
                "SELECT user_id FROM telegram_user_mappings WHERE telegram_chat_id = $1",
            )
            .bind(msg.chat.id.0)
            .fetch_optional(&db)
            .await
            {
                Ok(Some(row)) => {
                    let uid: Uuid = row.get(0);
                    Some(uid.to_string())
                }
                _ => None,
            };

            let user_id = match user_id_opt {
                Some(uid) => uid,
                None => {
                    bot.send_message(msg.chat.id, "❌ Your Telegram account is not linked to any exchange account. Use /link to see instructions on how to link your wallet.").await?;
                    return Ok(());
                }
            };

            let trading_service_url = std::env::var("TRADING_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:50052".to_string());

            let mut client =
                match proto::trading::trading_service_client::TradingServiceClient::connect(
                    trading_service_url,
                )
                .await
                {
                    Ok(c) => c,
                    Err(_) => {
                        bot.send_message(
                            msg.chat.id,
                            "❌ Failed to connect to Trading Service. Please try again later.",
                        )
                        .await?;
                        return Ok(());
                    }
                };

            let res = client
                .get_postions(proto::trading::GetPostionsRequest {
                    user_id: user_id.clone(),
                })
                .await;

            match res {
                Ok(r) => {
                    let positions = r.into_inner().positions;
                    if positions.is_empty() {
                        bot.send_message(msg.chat.id, "📊 **No Active Perp Positions**")
                            .await?;
                    } else {
                        let mut response_text =
                            "📊 **Your Active Perp Positions**:\n\n".to_string();
                        for pos in positions {
                            response_text.push_str(&format!(
                                "• **{}** ({})\n  Size: **{}**\n  Entry Price: **{}**\n  Leverage: **{}x** ({})\n  Unrealized PnL: **{}**\n\n",
                                pos.symbol, pos.side, pos.size, pos.entry_price, pos.leverage, pos.margin_mode, pos.unrealized_pnl
                            ));
                        }
                        bot.send_message(msg.chat.id, response_text).await?;
                    }
                }
                Err(_) => {
                    bot.send_message(msg.chat.id, "❌ Failed to retrieve positions.")
                        .await?;
                }
            }
        }
        Command::Trades => {
            let user_id_opt = match sqlx::query(
                "SELECT user_id FROM telegram_user_mappings WHERE telegram_chat_id = $1",
            )
            .bind(msg.chat.id.0)
            .fetch_optional(&db)
            .await
            {
                Ok(Some(row)) => {
                    let uid: Uuid = row.get(0);
                    Some(uid.to_string())
                }
                _ => None,
            };

            let user_id = match user_id_opt {
                Some(uid) => uid,
                None => {
                    bot.send_message(msg.chat.id, "❌ Your Telegram account is not linked to any exchange account. Use /link to see instructions on how to link your wallet.").await?;
                    return Ok(());
                }
            };

            let trading_service_url = std::env::var("TRADING_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:50052".to_string());

            let mut client =
                match proto::trading::trading_service_client::TradingServiceClient::connect(
                    trading_service_url,
                )
                .await
                {
                    Ok(c) => c,
                    Err(_) => {
                        bot.send_message(
                            msg.chat.id,
                            "❌ Failed to connect to Trading Service. Please try again later.",
                        )
                        .await?;
                        return Ok(());
                    }
                };

            let res = client
                .get_trade_history(proto::trading::GetTradeHistoryRequest {
                    user_id: user_id.clone(),
                })
                .await;

            match res {
                Ok(r) => {
                    let trades = r.into_inner().trades;
                    if trades.is_empty() {
                        bot.send_message(msg.chat.id, "📝 **No Trade History Found**")
                            .await?;
                    } else {
                        let mut response_text =
                            "📝 **Your Trade History (Last 10)**:\n\n".to_string();
                        let limit = trades.len().min(10);
                        for i in 0..limit {
                            let t = &trades[trades.len() - 1 - i];
                            response_text.push_str(&format!(
                                "• **{}** {} at **{}**\n  Qty: **{}**\n  Fee: **{}**\n  Time: {}\n\n",
                                t.symbol, t.side, t.price, t.quantity, t.fee, t.executed_at
                            ));
                        }
                        bot.send_message(msg.chat.id, response_text).await?;
                    }
                }
                Err(_) => {
                    bot.send_message(msg.chat.id, "❌ Failed to retrieve trade history.")
                        .await?;
                }
            }
        }
        Command::History => {
            let user_id_opt = match sqlx::query(
                "SELECT user_id FROM telegram_user_mappings WHERE telegram_chat_id = $1",
            )
            .bind(msg.chat.id.0)
            .fetch_optional(&db)
            .await
            {
                Ok(Some(row)) => {
                    let uid: Uuid = row.get(0);
                    Some(uid.to_string())
                }
                _ => None,
            };

            let user_id = match user_id_opt {
                Some(uid) => uid,
                None => {
                    bot.send_message(msg.chat.id, "❌ Your Telegram account is not linked to any exchange account. Use /link to see instructions on how to link your wallet.").await?;
                    return Ok(());
                }
            };

            let account_service_url = std::env::var("ACCOUNT_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:50053".to_string());

            let mut client =
                match proto::account::account_service_client::AccountServiceClient::connect(
                    account_service_url,
                )
                .await
                {
                    Ok(c) => c,
                    Err(_) => {
                        bot.send_message(
                            msg.chat.id,
                            "❌ Failed to connect to Account Service. Please try again later.",
                        )
                        .await?;
                        return Ok(());
                    }
                };

            let res = client
                .get_transaction_history(proto::account::GetTransactionHistoryRequest {
                    user_id: user_id.clone(),
                })
                .await;

            match res {
                Ok(r) => {
                    let txs = r.into_inner().transactions;
                    if txs.is_empty() {
                        bot.send_message(msg.chat.id, "📜 No Transaction History Found")
                            .await?;
                    } else {
                        fn fmt_status(status: &str) -> &str {
                            match status {
                                "SUCCESS" => "✅ Success",
                                "FAILED" => "❌ Failed",
                                "PENDING" => "⏳ Pending",
                                other => other,
                            }
                        }
                        fn fmt_tx(tx_hash: &str) -> String {
                            if tx_hash.is_empty() || tx_hash.starts_with("ERROR") {
                                "—".to_string()
                            } else if tx_hash.len() > 16 {
                                format!("{}…{}", &tx_hash[..8], &tx_hash[tx_hash.len() - 8..])
                            } else {
                                tx_hash.to_string()
                            }
                        }
                        fn fmt_ts(ts: &str) -> String {
                            ts.get(..16).unwrap_or(ts).replace('T', " ").to_string()
                        }
                        fn fmt_amt(amount: &str) -> String {
                            rust_decimal::Decimal::from_str(amount)
                                .map(|d| format!("{:.4}", d))
                                .unwrap_or_else(|_| amount.to_string())
                        }
                        fn tx_icon(tx_type: &str) -> &str {
                            match tx_type {
                                "DEPOSIT" => "⬇️",
                                "WITHDRAWAL" => "⬆️",
                                _ => "↔️",
                            }
                        }

                        let mut response_text = "📜 Account History (Last 10)\n".to_string();
                        response_text.push_str("──────────────────────\n");
                        let limit = txs.len().min(10);
                        for i in 0..limit {
                            let tx = &txs[txs.len() - 1 - i];
                            response_text.push_str(&format!(
                                "{} {} {}  {}\n🕐 {}\n🔗 {}\n\n",
                                tx_icon(&tx.transaction_type),
                                tx.transaction_type,
                                fmt_amt(&tx.amount),
                                tx.asset,
                                fmt_ts(&tx.created_at),
                                fmt_tx(&tx.tx_hash),
                            ));
                            response_text.push_str(&format!(
                                "   {}\n──────────────────────\n",
                                fmt_status(&tx.status)
                            ));
                        }
                        bot.send_message(msg.chat.id, response_text).await?;
                    }
                }
                Err(_) => {
                    bot.send_message(msg.chat.id, "❌ Failed to retrieve transaction history.")
                        .await?;
                }
            }
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

async fn run_kafka_notification_consumer(
    bot: Bot,
    db_pool: sqlx::Pool<sqlx::Postgres>,
) -> anyhow::Result<()> {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "telegram-bot-notifications-group")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .create()?;

    consumer.subscribe(&["user-notifications"])?;
    tracing::info!("Telegram Bot notifications consumer subscribed to user-notifications topic.");

    #[derive(Deserialize)]
    struct UserNotification {
        pub user_id: String,
        pub message: String,
    }

    let mut stream = consumer.stream();
    while let Some(msg_res) = stream.next().await {
        if let Ok(msg) = msg_res {
            if let Some(payload) = KafkaMessage::payload(&msg) {
                if let Ok(notif) = serde_json::from_slice::<UserNotification>(payload) {
                    let user_id = match Uuid::parse_str(&notif.user_id) {
                        Ok(u) => u,
                        Err(_) => continue,
                    };

                    let row_opt = sqlx::query(
                        "SELECT telegram_chat_id FROM telegram_user_mappings WHERE user_id = $1",
                    )
                    .bind(user_id)
                    .fetch_optional(&db_pool)
                    .await;

                    if let Ok(Some(row)) = row_opt {
                        let chat_id: i64 = row.get(0);
                        let _ = bot.send_message(ChatId(chat_id), notif.message).await;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn handle_start_command(
    bot: Bot,
    msg: Message,
    db: sqlx::Pool<sqlx::Postgres>,
    redis_client: redis::Client,
) -> ResponseResult<()> {
    let text = msg.text().unwrap_or_default();
    let parts: Vec<&str> = text.split_whitespace().collect();

    let app_url = std::env::var("TELEGRAM_MINI_APP_URL")
        .unwrap_or_else(|_| "https://dhruvilpatel.github.io/perps-tma-placeholder".to_string());

    let keyboard = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::web_app(
        "Open Perps App",
        WebAppInfo {
            url: app_url.parse().unwrap(),
        },
    )]]);

    if parts.len() == 2 {
        let token = parts[1];
        let redis_key = format!("telegram_token:{}", token);

        let mut redis_conn = match redis_client.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(_) => {
                let _ = bot
                    .send_message(
                        msg.chat.id,
                        "Failed to connect to Redis. Please try again later.",
                    )
                    .await;
                return Ok(());
            }
        };

        let user_id_res: Result<String, _> = redis_conn.get(&redis_key).await;
        if let Ok(user_id_str) = user_id_res {
            let user_id = match Uuid::parse_str(&user_id_str) {
                Ok(u) => u,
                Err(_) => {
                    let _ = bot
                        .send_message(msg.chat.id, "Invalid user UUID mapped to token.")
                        .await;
                    return Ok(());
                }
            };

            let query_res = sqlx::query(
                r#"
                INSERT INTO telegram_user_mappings (user_id, telegram_chat_id, created_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (user_id) DO UPDATE SET telegram_chat_id = EXCLUDED.telegram_chat_id
                "#,
            )
            .bind(user_id)
            .bind(msg.chat.id.0)
            .execute(&db)
            .await;

            let _: Result<(), _> = redis_conn.del(&redis_key).await;

            match query_res {
                Ok(_) => {
                    let _ = bot.send_message(
                        msg.chat.id,
                        "🎉 Account successfully linked! You will now receive push notifications on Telegram for all order executions."
                    )
                    .reply_markup(keyboard)
                    .await;
                    return Ok(());
                }
                Err(e) => {
                    let _ = bot
                        .send_message(msg.chat.id, format!("Failed to register: {:?}", e))
                        .await;
                    return Ok(());
                }
            }
        } else {
            let _ = bot.send_message(msg.chat.id, "Invalid or expired linking token. Please generate a new link from the exchange settings.").await;
            return Ok(());
        }
    }

    let _ = bot.send_message(
        msg.chat.id,
        "Welcome to the Perps Exchange! 📈\n\nClick the button below to launch the Trading Mini App and manage your order books, positions, and trades natively within Telegram."
    )
    .reply_markup(keyboard)
    .await;

    Ok(())
}
