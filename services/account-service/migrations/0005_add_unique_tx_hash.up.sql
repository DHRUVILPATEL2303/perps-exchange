CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_tx_hash_unique ON transactions (tx_hash) WHERE tx_hash IS NOT NULL;
