use uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
pub struct CustodyAddress {
    pub user_id: Uuid,
    pub pda_address: String,
    pub usdc_ata: String,
    pub usdt_ata: String,
}
