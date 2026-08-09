use crate::application::usecase::account_usecase::AccountUseCase;
use crate::domain::entities::account::Account;
use crate::domain::entities::custody_address::CustodyAddress;
use crate::domain::repositories::account_repository::AccountRepository;
use async_trait::async_trait;
use chrono::Utc;
use errors::app_error::ServiceError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rust_decimal::Decimal;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub struct AccountService {
    repository: Arc<dyn AccountRepository>,
    producer: Arc<FutureProducer>,
}

impl AccountService {
    pub fn new(repository: Arc<dyn AccountRepository>, producer: Arc<FutureProducer>) -> Self {
        Self {
            repository,
            producer,
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

    async fn get_transaction_history(
        &self,
        user_id: Uuid,
    ) -> Result<Vec<crate::domain::entities::transaction::Transaction>, ServiceError> {
        let txs = self.repository.list_transactions_by_user(user_id).await?;
        Ok(txs)
    }

    async fn get_or_create_custody_address(
        &self,
        user_id: Uuid,
    ) -> Result<CustodyAddress, ServiceError> {
        if let Some(custody) = self
            .repository
            .find_custody_address_by_user(user_id)
            .await?
        {
            return Ok(custody);
        }

        let program_id = Pubkey::from_str("2ayuWXRGujMmex2yJ4uoyMiFi1PUDU6yyhX9QAUJoVWL").unwrap();
        let (pda, _) =
            Pubkey::find_program_address(&[b"user_deposit", user_id.as_bytes()], &program_id);

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

        let _ = Pubkey::from_str(destination_address)
            .map_err(|e| ServiceError::Validation(format!("Invalid destination address: {}", e)))?;

        let (updated_account, tx_id) = self
            .repository
            .initiate_withdrawal(user_id, asset, amount)
            .await?;

        #[derive(serde::Serialize)]
        struct KafkaWithdrawalRequest {
            pub tx_id: Uuid,
            pub user_id: Uuid,
            pub asset: String,
            pub amount: String,
            pub destination_address: String,
        }

        let event = KafkaWithdrawalRequest {
            tx_id,
            user_id,
            asset: asset.to_string(),
            amount: amount.to_string(),
            destination_address: destination_address.to_string(),
        };

        let payload = serde_json::to_vec(&event).map_err(|e| {
            ServiceError::Validation(format!("Failed to serialize withdrawal event: {}", e))
        })?;

        let key = tx_id.to_string();

        self.producer
            .send(
                FutureRecord::to("withdrawal-requests")
                    .payload(&payload)
                    .key(key.as_bytes()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| {
                ServiceError::Validation(format!("Failed to send withdrawal to Kafka: {}", e))
            })?;

        Ok((tx_id.to_string(), updated_account.balance))
    }
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
