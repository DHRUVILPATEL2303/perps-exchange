CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    asset VARCHAR(16) NOT NULL,
    amount NUMERIC(38, 18) NOT NULL,
    transaction_type VARCHAR(16) NOT NULL,
    status VARCHAR(16) NOT NULL,
    tx_hash VARCHAR(128),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions (user_id);
