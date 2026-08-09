use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use sqlx::PgPool;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

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
    rpc_client: Arc<RpcClient>,
    cache: Arc<RwLock<CustodyCache>>,
    admin_keypair: Arc<Keypair>,
    program_id: Pubkey,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5432/postgres".to_string());
    let kafka_brokers =
        std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    let solana_rpc_url = std::env::var("SOLANA_RPC_URL").unwrap_or_else(|_| {
        "https://devnet.helius-rpc.com/?api-key=b07f07b6-4c5a-417d-9c31-93300c828917".to_string()
    });

    tracing::info!("Connecting to database...");
    let db_pool = PgPool::connect(&database_url).await?;

    tracing::info!("Connecting to Kafka...");
    let kafka_producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    tracing::info!("Connecting to Solana Devnet RPC: {}...", solana_rpc_url);
    let rpc_client = Arc::new(RpcClient::new(solana_rpc_url));

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

    let admin_keypair = get_or_create_keypair("/app/configs/custody-admin-keypair.json", "Admin")?;
    let program_id = Pubkey::from_str(
        &std::env::var("CUSTODY_PROGRAM_ID")
            .unwrap_or_else(|_| "2ayuWXRGujMmex2yJ4uoyMiFi1PUDU6yyhX9QAUJoVWL".to_string()),
    )?;

    let (state_pda, bump) = Pubkey::find_program_address(&[b"custody_state_v8"], &program_id);
    tracing::info!("Derived State PDA: {}, bump: {}", state_pda, bump);

    let treasury_usdc_str = std::env::var("CUSTODY_TREASURY_USDC_ATA")
        .unwrap_or_else(|_| "7zCsbfCpT13QzF9KCKbfvJHsxPyYwv1s7kJ6ZhXtrVsC".to_string());
    let treasury_usdc = Pubkey::from_str(&treasury_usdc_str)?;

    let treasury_usdt_str = std::env::var("CUSTODY_TREASURY_USDT_ATA")
        .unwrap_or_else(|_| "DjmHU8he415YqSNwnQobhGNX6cmj7ao6uZ64pRtBXPZb".to_string());
    let treasury_usdt = Pubkey::from_str(&treasury_usdt_str)?;

    tracing::info!("Ensuring contract state is initialized...");
    if let Err(e) = ensure_contract_state_initialized(
        &rpc_client,
        &admin_keypair,
        &state_pda,
        bump,
        &program_id,
        &treasury_usdc,
        &treasury_usdt,
    ) {
        tracing::error!("Failed to initialize contract state: {:?}", e);
    }

    let state = Arc::new(AppState {
        db_pool,
        kafka_producer,
        kafka_topic: "solana-deposits".to_string(),
        rpc_client,
        cache,
        admin_keypair: Arc::new(admin_keypair),
        program_id,
    });

    tracing::info!("Starting blockchain polling loop...");
    loop {
        if let Err(e) = poll_deposits(state.clone()).await {
            tracing::error!("Error polling deposits: {:?}", e);
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn refresh_custody_cache(
    db: &PgPool,
    cache: &Arc<RwLock<CustodyCache>>,
    is_startup: bool,
) -> Result<()> {
    let rows = sqlx::query_as::<_, CustodyRow>(
        "SELECT user_id, usdc_ata, usdt_ata FROM custody_addresses",
    )
    .fetch_all(db)
    .await?;

    let mut write_guard = cache.write().await;
    for row in rows {
        let user_id = row.user_id;

        if !write_guard.ata_map.contains_key(&row.usdc_ata) {
            tracing::info!(
                "Cached new USDC custody ATA: {} for user {}",
                row.usdc_ata,
                user_id
            );
            write_guard.ata_map.insert(
                row.usdc_ata.clone(),
                CustodyInfo {
                    user_id,
                    asset: "USDC".to_string(),
                    current_balance: 0,
                    is_initialized: !is_startup, // If not startup, mark initialized with 0 balance
                },
            );
        }

        if !write_guard.ata_map.contains_key(&row.usdt_ata) {
            tracing::info!(
                "Cached new USDT custody ATA: {} for user {}",
                row.usdt_ata,
                user_id
            );
            write_guard.ata_map.insert(
                row.usdt_ata.clone(),
                CustodyInfo {
                    user_id,
                    asset: "USDT".to_string(),
                    current_balance: 0,
                    is_initialized: !is_startup, // If not startup, mark initialized with 0 balance
                },
            );
        }
    }

    tracing::info!(
        "Custody cache sync completed. Active monitored accounts: {}",
        write_guard.ata_map.len()
    );
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
        let pubkeys: Vec<Pubkey> = chunk
            .iter()
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
                                    ata_str,
                                    info.asset,
                                    onchain_balance
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
                                    ata_str,
                                    info.asset,
                                    info.current_balance,
                                    onchain_balance
                                );
                                info.current_balance = onchain_balance;
                            }
                        }
                    }
                } else {
                    let mut write_guard = state.cache.write().await;
                    if let Some(info) = write_guard.ata_map.get_mut(&ata_str) {
                        if !info.is_initialized {
                            tracing::info!(
                                "Monitored account {} ({}) is not yet initialized on-chain.",
                                ata_str,
                                info.asset
                            );
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
        user_id,
        amount_decimal,
        asset,
        tx_hash
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

        if let Err(e) = state
            .kafka_producer
            .send(record, Duration::from_secs(5))
            .await
        {
            tracing::error!("Failed to send deposit to Kafka: {:?}", e);
        } else {
            let state_clone = state.clone();
            let asset_clone = event.asset.clone();
            let user_ata_clone = ata_str.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    trigger_onchain_sweep(state_clone, user_id, asset_clone, user_ata_clone).await
                {
                    tracing::error!("Failed to sweep deposit on-chain: {:?}", e);
                }
            });
        }
    }
}

async fn trigger_onchain_sweep(
    state: Arc<AppState>,
    user_id: Uuid,
    asset: String,
    user_ata_str: String,
) -> Result<()> {
    tracing::info!(
        "Triggering automated on-chain sweep for user {}'s {} ATA: {}",
        user_id,
        asset,
        user_ata_str
    );

    let program_id = state.program_id;
    let admin_keypair = state.admin_keypair.clone();
    let (state_pda, _) = Pubkey::find_program_address(&[b"custody_state_v8"], &program_id);

    let treasury_ata_str = if asset == "USDC" {
        std::env::var("CUSTODY_TREASURY_USDC_ATA")
            .unwrap_or_else(|_| "7zCsbfCpT13QzF9KCKbfvJHsxPyYwv1s7kJ6ZhXtrVsC".to_string())
    } else {
        std::env::var("CUSTODY_TREASURY_USDT_ATA")
            .unwrap_or_else(|_| "DjmHU8he415YqSNwnQobhGNX6cmj7ao6uZ64pRtBXPZb".to_string())
    };
    let treasury_ata = Pubkey::from_str(&treasury_ata_str)?;

    let spl_token_program_id = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")?;

    let (user_deposit_pda, _) =
        Pubkey::find_program_address(&[b"user_deposit", user_id.as_bytes()], &program_id);

    let metas = vec![
        solana_sdk::instruction::AccountMeta::new(state_pda, false),
        solana_sdk::instruction::AccountMeta::new(admin_keypair.pubkey(), true),
        solana_sdk::instruction::AccountMeta::new(Pubkey::from_str(&user_ata_str)?, false),
        solana_sdk::instruction::AccountMeta::new(treasury_ata, false),
        solana_sdk::instruction::AccountMeta::new_readonly(spl_token_program_id, false),
        solana_sdk::instruction::AccountMeta::new_readonly(user_deposit_pda, false), // 6th: user deposit PDA authority
    ];

    let mut data = vec![1]; // sweep instruction index
    data.extend_from_slice(user_id.as_bytes());

    let sweep_ix = solana_sdk::instruction::Instruction::new_with_bytes(program_id, &data, metas);

    let transaction = solana_sdk::transaction::Transaction::new_with_payer(
        &[sweep_ix],
        Some(&admin_keypair.pubkey()),
    );

    let rpc = state.rpc_client.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let blockhash = rpc.get_latest_blockhash()?;
        let mut tx = transaction;
        tx.sign(&[&admin_keypair], blockhash);

        let sig = rpc.send_and_confirm_transaction(&tx)?;
        tracing::info!("On-chain sweep successful! Tx signature: {}", sig);
        Ok(())
    })
    .await??;

    Ok(())
}

fn get_or_create_keypair(path: &str, name: &str) -> Result<Keypair> {
    if Path::new(path).exists() {
        let content = fs::read_to_string(path)?;
        let bytes: Vec<u8> = serde_json::from_str(&content)?;
        let keypair = Keypair::from_bytes(&bytes)?;
        tracing::info!(
            "Loaded existing keypair for {} from {}. Public key: {}",
            name,
            path,
            keypair.pubkey()
        );
        Ok(keypair)
    } else {
        let keypair = Keypair::new();
        let bytes = keypair.to_bytes().to_vec();
        let content = serde_json::to_string(&bytes)?;
        if let Some(parent) = Path::new(path).parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, content)?;
        tracing::info!(
            "Generated new keypair for {} at {}. Public key: {}",
            name,
            path,
            keypair.pubkey()
        );
        Ok(keypair)
    }
}

fn ensure_contract_state_initialized(
    rpc_client: &RpcClient,
    admin_keypair: &Keypair,
    state_pda: &Pubkey,
    bump: u8,
    program_id: &Pubkey,
    treasury_usdc_ata: &Pubkey,
    treasury_usdt_ata: &Pubkey,
) -> Result<()> {
    match rpc_client.get_account(state_pda) {
        Ok(_) => {
            tracing::info!(
                "Custody state account {} already initialized on-chain.",
                state_pda
            );
            Ok(())
        }
        Err(_) => {
            tracing::info!(
                "Custody state account {} not found on-chain. Initializing...",
                state_pda
            );

            let rent_exemption = rpc_client.get_minimum_balance_for_rent_exemption(96)?;

            let mut init_data = vec![0u8];
            init_data.extend_from_slice(treasury_usdc_ata.as_ref());
            init_data.extend_from_slice(treasury_usdt_ata.as_ref());
            init_data.extend_from_slice(&rent_exemption.to_le_bytes());
            init_data.push(bump);

            let init_metas = vec![
                solana_sdk::instruction::AccountMeta::new(*state_pda, false),
                solana_sdk::instruction::AccountMeta::new(admin_keypair.pubkey(), true),
                solana_sdk::instruction::AccountMeta::new_readonly(
                    solana_sdk::system_program::id(),
                    false,
                ),
            ];

            let init_ix = solana_sdk::instruction::Instruction::new_with_bytes(
                *program_id,
                &init_data,
                init_metas,
            );

            let transaction = solana_sdk::transaction::Transaction::new_with_payer(
                &[init_ix],
                Some(&admin_keypair.pubkey()),
            );

            let blockhash = rpc_client.get_latest_blockhash()?;
            let mut tx = transaction;
            tx.sign(&[admin_keypair], blockhash);

            let sig = rpc_client.send_and_confirm_transaction(&tx)?;
            tracing::info!(
                "Successfully created and initialized custody state account on-chain! Tx signature: {}",
                sig
            );
            Ok(())
        }
    }
}
