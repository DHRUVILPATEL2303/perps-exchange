use anyhow::{Context, Result};
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use sqlx::{Connection, Pool, Postgres};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[derive(Deserialize)]
struct KafkaWithdrawalRequest {
    pub tx_id: Uuid,
    pub user_id: Uuid,
    pub asset: String,
    pub amount: String,
    pub destination_address: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();

    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5432/perps_accounts".to_string()
    });

    let db_pool = sqlx::PgPool::connect(&database_url)
        .await
        .context("Failed to connect to database")?;

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "withdrawal-signer-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
        .create()
        .context("Failed to create Kafka consumer")?;

    consumer.subscribe(&["withdrawal-requests"])?;

    let solana_rpc_url = std::env::var("SOLANA_RPC_URL").unwrap_or_else(|_| {
        "https://devnet.helius-rpc.com/?api-key=b07f07b6-4c5a-417d-9c31-93300c828917".to_string()
    });
    let rpc_client = Arc::new(RpcClient::new(solana_rpc_url));

    let keypair_path = std::env::var("CUSTODY_ADMIN_KEYPAIR_PATH")
        .unwrap_or_else(|_| "./configs/custody-admin-keypair.json".to_string());
    let admin_keypair =
        Arc::new(load_keypair(&keypair_path).context("Failed to load admin keypair")?);

    tracing::info!("Withdrawal Signer Service started. Listening for requests...");

    let mut stream = consumer.stream();
    while let Some(msg_result) = stream.next().await {
        match msg_result {
            Err(e) => {
                tracing::error!("Kafka consumer error: {}", e);
            }
            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    if let Ok(req) = serde_json::from_slice::<KafkaWithdrawalRequest>(payload) {
                        let rpc = rpc_client.clone();
                        let admin = admin_keypair.clone();
                        let db = db_pool.clone();
                        tokio::spawn(async move {
                            if let Err(e) = process_withdrawal(req, rpc, admin, db).await {
                                tracing::error!("Failed to process withdrawal: {:?}", e);
                            }
                        });
                        let _ = consumer.commit_message(&msg, CommitMode::Async);
                    }
                }
            }
        }
    }

    Ok(())
}

async fn process_withdrawal(
    req: KafkaWithdrawalRequest,
    rpc: Arc<RpcClient>,
    admin: Arc<Keypair>,
    db: Pool<Postgres>,
) -> Result<()> {
    tracing::info!(
        "Processing withdrawal {} for user {}",
        req.tx_id,
        req.user_id
    );

    let user_dest_pubkey = Pubkey::from_str(&req.destination_address)?;
    let amount = Decimal::from_str(&req.amount)?;

    let amount_multiplier = Decimal::from(1_000_000u64);
    let mut rounded = amount * amount_multiplier;
    rounded.rescale(0);
    let amount_base_units = rounded.to_string().parse::<u64>()?;

    let usdc_mint = Pubkey::from_str("4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU").unwrap();
    let usdt_mint = Pubkey::from_str("EJwZeg1u717JhEv6YoRrt8A6gGTLrmKWJxgB7P15fTo3").unwrap();
    let mint_pubkey = if req.asset == "USDC" {
        usdc_mint
    } else {
        usdt_mint
    };

    let treasury_ata_env = if req.asset == "USDC" {
        std::env::var("CUSTODY_TREASURY_USDC_ATA")
            .unwrap_or_else(|_| "7zCsbfCpT13QzF9KCKbfvJHsxPyYwv1s7kJ6ZhXtrVsC".to_string())
    } else {
        std::env::var("CUSTODY_TREASURY_USDT_ATA")
            .unwrap_or_else(|_| "DjmHU8he415YqSNwnQobhGNX6cmj7ao6uZ64pRtBXPZb".to_string())
    };
    let treasury_ata = Pubkey::from_str(&treasury_ata_env)?;

    let user_dest_ata = get_associated_token_address(&user_dest_pubkey, &mint_pubkey);

    let spl_token_program_id =
        Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
    let spl_associated_token_program_id =
        Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").unwrap();

    let rpc_res = tokio::task::spawn_blocking(move || {
        let account_exists = rpc.get_account(&user_dest_ata).is_ok();

        let mut instructions = Vec::new();
        if !account_exists {
            let create_ata_ix = solana_sdk::instruction::Instruction {
                program_id: spl_associated_token_program_id,
                accounts: vec![
                    solana_sdk::instruction::AccountMeta::new(admin.pubkey(), true),
                    solana_sdk::instruction::AccountMeta::new(user_dest_ata, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(user_dest_pubkey, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(mint_pubkey, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(
                        solana_sdk::system_program::id(),
                        false,
                    ),
                    solana_sdk::instruction::AccountMeta::new_readonly(spl_token_program_id, false),
                ],
                data: vec![],
            };
            instructions.push(create_ata_ix);
        }

        let mut transfer_data = [0u8; 9];
        transfer_data[0] = 3;
        transfer_data[1..9].copy_from_slice(&amount_base_units.to_le_bytes());

        let transfer_ix = solana_sdk::instruction::Instruction {
            program_id: spl_token_program_id,
            accounts: vec![
                solana_sdk::instruction::AccountMeta::new(treasury_ata, false),
                solana_sdk::instruction::AccountMeta::new(user_dest_ata, false),
                solana_sdk::instruction::AccountMeta::new(admin.pubkey(), true),
            ],
            data: transfer_data.to_vec(),
        };
        instructions.push(transfer_ix);

        let transaction = solana_sdk::transaction::Transaction::new_with_payer(
            &instructions,
            Some(&admin.pubkey()),
        );

        let blockhash = rpc.get_latest_blockhash()?;
        let mut tx = transaction;
        tx.sign(&[&*admin], blockhash);

        let sig = rpc.send_and_confirm_transaction(&tx)?;
        Ok::<String, solana_client::client_error::ClientError>(sig.to_string())
    })
    .await;

    match rpc_res {
        Ok(Ok(sig)) => {
            sqlx::query("UPDATE transactions SET status = 'SUCCESS', tx_hash = $1 WHERE id = $2")
                .bind(&sig)
                .bind(req.tx_id)
                .execute(&db)
                .await?;
            tracing::info!("On-chain withdrawal transfer successful! Tx: {}", sig);
        }
        Ok(Err(e)) => {
            let err_msg = format!("Solana RPC transfer failed: {:?}", e);
            tracing::error!("{}", err_msg);
            revert_withdrawal(req.user_id, &req.asset, amount, req.tx_id, &err_msg, &db).await?;
        }
        Err(e) => {
            let err_msg = format!("Task execution failed: {:?}", e);
            tracing::error!("{}", err_msg);
            revert_withdrawal(req.user_id, &req.asset, amount, req.tx_id, &err_msg, &db).await?;
        }
    }

    Ok(())
}

async fn revert_withdrawal(
    user_id: Uuid,
    asset: &str,
    amount: Decimal,
    tx_id: Uuid,
    error_msg: &str,
    db: &Pool<Postgres>,
) -> Result<()> {
    let mut tx = db.begin().await?;

    let account_opt =
        sqlx::query("SELECT balance FROM accounts WHERE user_id = $1 AND asset = $2 FOR UPDATE")
            .bind(user_id)
            .bind(asset)
            .fetch_optional(&mut *tx)
            .await?;

    if let Some(row) = account_opt {
        let balance: Decimal = sqlx::Row::get(&row, 0);
        let new_balance = balance + amount;

        sqlx::query(
            "UPDATE accounts SET balance = $1, updated_at = NOW() WHERE user_id = $2 AND asset = $3"
        )
        .bind(new_balance)
        .bind(user_id)
        .bind(asset)
        .execute(&mut *tx)
        .await?;
    }

    let err_truncated = error_msg.chars().take(512).collect::<String>();
    sqlx::query(
        "UPDATE transactions SET status = 'FAILED', tx_hash = NULL, error_message = $1 WHERE id = $2"
    )
    .bind(err_truncated)
    .bind(tx_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

fn get_associated_token_address(wallet_address: &Pubkey, token_mint_address: &Pubkey) -> Pubkey {
    let spl_associated_token_program_id =
        Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").unwrap();
    let spl_token_program_id =
        Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();

    let (ata, _) = Pubkey::find_program_address(
        &[
            wallet_address.as_ref(),
            spl_token_program_id.as_ref(),
            token_mint_address.as_ref(),
        ],
        &spl_associated_token_program_id,
    );
    ata
}

fn load_keypair(path: &str) -> Result<Keypair> {
    let file = std::fs::File::open(path)?;
    let bytes: Vec<u8> = serde_json::from_reader(file)?;
    let keypair = Keypair::from_bytes(&bytes)
        .map_err(|e| anyhow::anyhow!("Invalid keypair bytes: {:?}", e))?;
    Ok(keypair)
}
