CREATE TABLE IF NOT EXISTS funding_payments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    side VARCHAR(16) NOT NULL,
    position_size NUMERIC(38, 18) NOT NULL,
    funding_rate NUMERIC(38, 18) NOT NULL,
    amount NUMERIC(38, 18) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_funding_payments_user_id ON funding_payments (user_id);
