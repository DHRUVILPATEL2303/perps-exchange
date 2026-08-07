use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use sqlx::PgPool;
use tokio::sync::RwLock;
use uuid::Uuid;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

struct CustodyInfo {
    pub user_id: Uuid,
    pub asset: String,
    pub current_balance: u64,
    pub is_initialized: bool,
}

#[derive(sqlx::FromRow)]
struct CustodyRow {
    pub user_id: Uuid,
    pub usdc_ata: String,
    pub usdt_ata: String,
}

struct CustodyCache {
    ata_map: HashMap<String, CustodyInfo>,
}

#[derive(Serialize)]
struct SolanaDepositEvent {
    pub user_id: Uuid,
    pub amount: String,
    pub asset: String,
    pub tx_hash: String,
}

struct AppState {
    db_pool: PgPool,
    kafka_producer: FutureProducer,
    kafka_topic: String,
    rpc_client: RpcClient,
    cache: Arc<RwLock<CustodyCache>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5432/postgres".to_string());
    let kafka_brokers = std::env::var("KAFKA_BROKERS")
        .unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    let solana_rpc_url = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "https://devnet.helius-rpc.com/?api-key=b07f07b6-4c5a-417d-9c31-93300c828917".to_string());

    tracing::info!("Connecting to database...");
    let db_pool = PgPool::connect(&database_url).await?;

    tracing::info!("Connecting to Kafka...");
    let kafka_producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    tracing::info!("Connecting to Solana Devnet RPC: {}...", solana_rpc_url);
    let rpc_client = RpcClient::new(solana_rpc_url);

    let cache = Arc::new(RwLock::new(CustodyCache {
        ata_map: HashMap::new(),
    }));

    let db_clone = db_pool.clone();
    let cache_clone = cache.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if let Err(e) = refresh_custody_cache(&db_clone, &cache_clone, false).await {
                tracing::error!("Failed to refresh custody cache: {:?}", e);
            }
        }
    });

    tracing::info!("Initializing custody cache at startup...");
    refresh_custody_cache(&db_pool, &cache, true).await?;

    let state = Arc::new(AppState {
        db_pool,
        kafka_producer,
        kafka_topic: "solana-deposits".to_string(),
        rpc_client,
        cache,
    });

    tracing::info!("Starting blockchain polling loop...");
    loop {
        if let Err(e) = poll_deposits(state.clone()).await {
            tracing::error!("Error polling deposits: {:?}", e);
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn refresh_custody_cache(db: &PgPool, cache: &Arc<RwLock<CustodyCache>>, is_startup: bool) -> Result<()> {
    let rows = sqlx::query_as::<_, CustodyRow>(
        "SELECT user_id, usdc_ata, usdt_ata FROM custody_addresses"
    )
    .fetch_all(db)
    .await?;

    let mut write_guard = cache.write().await;
    for row in rows {
        let user_id = row.user_id;
        
        if !write_guard.ata_map.contains_key(&row.usdc_ata) {
            tracing::info!("Cached new USDC custody ATA: {} for user {}", row.usdc_ata, user_id);
            write_guard.ata_map.insert(row.usdc_ata.clone(), CustodyInfo {
                user_id,
                asset: "USDC".to_string(),
                current_balance: 0,
                is_initialized: !is_startup, // If not startup, mark initialized with 0 balance
            });
        }

        if !write_guard.ata_map.contains_key(&row.usdt_ata) {
            tracing::info!("Cached new USDT custody ATA: {} for user {}", row.usdt_ata, user_id);
            write_guard.ata_map.insert(row.usdt_ata.clone(), CustodyInfo {
                user_id,
                asset: "USDT".to_string(),
                current_balance: 0,
                is_initialized: !is_startup, // If not startup, mark initialized with 0 balance
            });
        }
    }

    tracing::info!("Custody cache sync completed. Active monitored accounts: {}", write_guard.ata_map.len());
    Ok(())
}

async fn poll_deposits(state: Arc<AppState>) -> Result<()> {
    let keys: Vec<String> = {
        let read_guard = state.cache.read().await;
        read_guard.ata_map.keys().cloned().collect()
    };

    if keys.is_empty() {
        return Ok(());
    }

    tracing::info!("Polling {} custody accounts for deposits...", keys.len());

    for chunk in keys.chunks(100) {
        let pubkeys: Vec<Pubkey> = chunk.iter()
            .filter_map(|k| Pubkey::from_str(k).ok())
            .collect();

        if let Ok(accounts) = state.rpc_client.get_multiple_accounts(&pubkeys) {
            for (idx, account_opt) in accounts.into_iter().enumerate() {
                let ata_str = chunk[idx].clone();
                if let Some(account) = account_opt {
                    if account.data.len() >= 72 {
                        let mut amount_bytes = [0u8; 8];
                        amount_bytes.copy_from_slice(&account.data[64..72]);
                        let onchain_balance = u64::from_le_bytes(amount_bytes);

                        let mut write_guard = state.cache.write().await;
                        if let Some(info) = write_guard.ata_map.get_mut(&ata_str) {
                            if !info.is_initialized {
                                tracing::info!(
                                    "Initialized balance cache for {} ({}): {} base units",
                                    ata_str, info.asset, onchain_balance
                                );
                                info.current_balance = onchain_balance;
                                info.is_initialized = true;
                            } else if onchain_balance > info.current_balance {
                                let diff = onchain_balance - info.current_balance;
                                tracing::info!(
                                    "Detected new balance increase for {} ({}): {} -> {} (diff: {})",
                                    ata_str, info.asset, info.current_balance, onchain_balance, diff
                                );
                                info.current_balance = onchain_balance;

                                let user_id = info.user_id;
                                let asset = info.asset.clone();

                                tokio::spawn(handle_detected_deposit(
                                    state.clone(),
                                    user_id,
                                    asset,
                                    diff,
                                    ata_str,
                                ));
                            } else if onchain_balance < info.current_balance {
                                tracing::info!(
                                    "Balance decreased for {} ({}): {} -> {} (account swept)",
                                    ata_str, info.asset, info.current_balance, onchain_balance
                                );
                                info.current_balance = onchain_balance;
                            }
                        }
                    }
                } else {
                    let mut write_guard = state.cache.write().await;
                    if let Some(info) = write_guard.ata_map.get_mut(&ata_str) {
                        if !info.is_initialized {
                            tracing::info!("Monitored account {} ({}) is not yet initialized on-chain.", ata_str, info.asset);
                            info.is_initialized = true;
                        }
                    }
                }
            }
        } else {
            tracing::error!("Failed to fetch multiple accounts from RPC.");
        }
    }

    Ok(())
}

async fn handle_detected_deposit(
    state: Arc<AppState>,
    user_id: Uuid,
    asset: String,
    diff: u64,
    ata_str: String,
) {
    let mut tx_hash = "UNKNOWN_TX_SIGNATURE".to_string();
    if let Ok(pubkey) = Pubkey::from_str(&ata_str) {
        if let Ok(sigs) = state.rpc_client.get_signatures_for_address(&pubkey) {
            if let Some(latest) = sigs.first() {
                tx_hash = latest.signature.clone();
            }
        }
    }

    let amount_decimal = rust_decimal::Decimal::new(diff as i64, 6);
    
    tracing::info!(
        "Processing deposit: user {} deposited {} {} via tx {}",
        user_id, amount_decimal, asset, tx_hash
    );

    let event = SolanaDepositEvent {
        user_id,
        amount: amount_decimal.to_string(),
        asset,
        tx_hash,
    };

    let user_id_str = user_id.to_string();
    if let Ok(payload) = serde_json::to_string(&event) {
        let record = FutureRecord::to(&state.kafka_topic)
            .key(&user_id_str)
            .payload(&payload);

        if let Err(e) = state.kafka_producer.send(record, Duration::from_secs(5)).await {
            tracing::error!("Failed to send deposit to Kafka: {:?}", e);
        }
    }
}
