use crate::application::usecase::account_usecase::AccountUseCase;
use crate::domain::entities::account::Account;
use crate::domain::entities::custody_address::CustodyAddress;
use crate::domain::repositories::account_repository::AccountRepository;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use async_trait::async_trait;
use chrono::Utc;
use errors::app_error::ServiceError;
use rust_decimal::Decimal;
use std::sync::Arc;
use uuid::Uuid;

use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;

pub struct AccountService {
    repository: Arc<dyn AccountRepository>,
    rpc_client: Arc<RpcClient>,
    admin_keypair: Arc<Keypair>,
}

impl AccountService {
    pub fn new(
        repository: Arc<dyn AccountRepository>,
        rpc_client: Arc<RpcClient>,
        admin_keypair: Arc<Keypair>,
    ) -> Self {
        Self {
            repository,
            rpc_client,
            admin_keypair,
        }
    }
}

#[async_trait]
impl AccountUseCase for AccountService {
    async fn get_balance(&self, user_id: Uuid, asset: &str) -> Result<Account, ServiceError> {
        if let Some(account) = self
            .repository
            .find_by_user_and_asset(user_id, asset)
            .await?
        {
            Ok(account)
        } else {
            let new_account = Account {
                id: Uuid::new_v4(),
                user_id,
                asset: asset.to_string(),
                balance: Decimal::ZERO,
                frozen: Decimal::ZERO,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };
            let created = self.repository.create(new_account).await?;
            Ok(created)
        }
    }

    async fn lock_margin(
        &self,
        user_id: Uuid,
        asset: &str,
        amount: Decimal,
    ) -> Result<(), ServiceError> {
        self.repository
            .lock_margin_atomic(user_id, asset, amount)
            .await?;
        Ok(())
    }

    async fn release_margin(
        &self,
        user_id: Uuid,
        asset: &str,
        amount: Decimal,
    ) -> Result<(), ServiceError> {
        self.repository
            .release_margin_atomic(user_id, asset, amount)
            .await?;
        Ok(())
    }

    async fn adjust_margin(
        &self,
        user_id: Uuid,
        asset: &str,
        amount: Decimal,
        adjustment_type: &str,
        tx_hash: Option<String>,
    ) -> Result<Account, ServiceError> {
        let updated = self
            .repository
            .adjust_margin_atomic(user_id, asset, amount, adjustment_type, tx_hash)
            .await?;
        Ok(updated)
    }

    async fn get_transaction_history(&self, user_id: Uuid) -> Result<Vec<crate::domain::entities::transaction::Transaction>, ServiceError> {
        let txs = self.repository.list_transactions_by_user(user_id).await?;
        Ok(txs)
    }

    async fn get_or_create_custody_address(&self, user_id: Uuid) -> Result<CustodyAddress, ServiceError> {
        if let Some(custody) = self.repository.find_custody_address_by_user(user_id).await? {
            return Ok(custody);
        }

        let program_id = Pubkey::from_str("2ayuWXRGujMmex2yJ4uoyMiFi1PUDU6yyhX9QAUJoVWL").unwrap();
        let (pda, _) = Pubkey::find_program_address(
            &[b"user_deposit", user_id.as_bytes()],
            &program_id,
        );

        let usdc_mint = Pubkey::from_str("4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU").unwrap();
        let usdt_mint = Pubkey::from_str("EJwZeg1u717JhEv6YoRrt8A6gGTLrmKWJxgB7P15fTo3").unwrap();

        let usdc_ata = get_associated_token_address(&pda, &usdc_mint);
        let usdt_ata = get_associated_token_address(&pda, &usdt_mint);

        let new_custody = CustodyAddress {
            user_id,
            pda_address: pda.to_string(),
            usdc_ata: usdc_ata.to_string(),
            usdt_ata: usdt_ata.to_string(),
        };

        let saved = self.repository.save_custody_address(new_custody).await?;
        Ok(saved)
    }

    async fn withdraw_funds(
        &self,
        user_id: Uuid,
        asset: &str,
        amount: Decimal,
        destination_address: &str,
    ) -> Result<(String, Decimal), ServiceError> {
        if asset != "USDC" && asset != "USDT" {
            return Err(ServiceError::Validation("Unsupported asset".to_string()));
        }

        let user_dest_pubkey = Pubkey::from_str(destination_address)
            .map_err(|e| ServiceError::Validation(format!("Invalid destination address: {}", e)))?;

        let (updated_account, tx_id) = self
            .repository
            .initiate_withdrawal(user_id, asset, amount)
            .await?;

        let amount_multiplier = Decimal::from(1_000_000u64);
        let mut rounded = amount * amount_multiplier;
        rounded.rescale(0);
        let amount_base_units = rounded.to_string().parse::<u64>().map_err(|e| {
            ServiceError::Validation(format!("Invalid amount format: {}", e))
        })?;

        let usdc_mint = Pubkey::from_str("4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU").unwrap();
        let usdt_mint = Pubkey::from_str("EJwZeg1u717JhEv6YoRrt8A6gGTLrmKWJxgB7P15fTo3").unwrap();
        let mint_pubkey = if asset == "USDC" { usdc_mint } else { usdt_mint };

        let treasury_ata_env = if asset == "USDC" {
            std::env::var("CUSTODY_TREASURY_USDC_ATA")
                .unwrap_or_else(|_| "7zCsbfCpT13QzF9KCKbfvJHsxPyYwv1s7kJ6ZhXtrVsC".to_string())
        } else {
            std::env::var("CUSTODY_TREASURY_USDT_ATA")
                .unwrap_or_else(|_| "DjmHU8he415YqSNwnQobhGNX6cmj7ao6uZ64pRtBXPZb".to_string())
        };
        let treasury_ata = Pubkey::from_str(&treasury_ata_env).map_err(|e| {
            ServiceError::Validation(format!("Invalid treasury ATA in env: {}", e))
        })?;

        let user_dest_ata = get_associated_token_address(&user_dest_pubkey, &mint_pubkey);

        let spl_token_program_id = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let spl_associated_token_program_id = Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").unwrap();

        let rpc = self.rpc_client.clone();
        let admin_keypair = self.admin_keypair.clone();

        let rpc_res = tokio::task::spawn_blocking(move || {
            let account_exists = rpc.get_account(&user_dest_ata).is_ok();
            
            let mut instructions = Vec::new();
            if !account_exists {
                let create_ata_ix = solana_sdk::instruction::Instruction {
                    program_id: spl_associated_token_program_id,
                    accounts: vec![
                        solana_sdk::instruction::AccountMeta::new(admin_keypair.pubkey(), true),
                        solana_sdk::instruction::AccountMeta::new(user_dest_ata, false),
                        solana_sdk::instruction::AccountMeta::new_readonly(user_dest_pubkey, false),
                        solana_sdk::instruction::AccountMeta::new_readonly(mint_pubkey, false),
                        solana_sdk::instruction::AccountMeta::new_readonly(solana_sdk::system_program::id(), false),
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
                    solana_sdk::instruction::AccountMeta::new(admin_keypair.pubkey(), true),
                ],
                data: transfer_data.to_vec(),
            };
            instructions.push(transfer_ix);

            let transaction = solana_sdk::transaction::Transaction::new_with_payer(
                &instructions,
                Some(&admin_keypair.pubkey()),
            );

            let blockhash = rpc.get_latest_blockhash()?;
            let mut tx = transaction;
            tx.sign(&[&*admin_keypair], blockhash);

            let sig = rpc.send_and_confirm_transaction(&tx)?;
            Ok::<String, solana_client::client_error::ClientError>(sig.to_string())
        })
        .await;

        match rpc_res {
            Ok(Ok(sig)) => {
                self.repository
                    .update_transaction_status_and_hash(tx_id, "SUCCESS", Some(sig.clone()))
                    .await?;
                tracing::info!("On-chain withdrawal transfer successful! Tx: {}", sig);
                Ok((sig, updated_account.balance))
            }
            Ok(Err(e)) => {
                let err_msg = format!("Solana RPC transfer failed: {:?}", e);
                tracing::error!("{}", err_msg);
                self.repository
                    .revert_withdrawal(user_id, asset, amount, tx_id, &err_msg)
                    .await?;
                Err(ServiceError::Validation(err_msg))
            }
            Err(e) => {
                let err_msg = format!("Task execution failed: {:?}", e);
                tracing::error!("{}", err_msg);
                self.repository
                    .revert_withdrawal(user_id, asset, amount, tx_id, &err_msg)
                    .await?;
                Err(ServiceError::Validation(err_msg))
            }
        }
    }
}

fn get_associated_token_address(wallet_address: &Pubkey, token_mint_address: &Pubkey) -> Pubkey {
    let spl_associated_token_program_id = Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").unwrap();
    let spl_token_program_id = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
    
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
