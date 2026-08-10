CREATE TABLE IF NOT EXISTS realized_pnl_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    side VARCHAR(16) NOT NULL,
    qty NUMERIC(38, 18) NOT NULL,
    entry_price NUMERIC(38, 18) NOT NULL,
    exit_price NUMERIC(38, 18) NOT NULL,
    realized_pnl NUMERIC(38, 18) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_realized_pnl_history_user_id ON realized_pnl_history (user_id);
