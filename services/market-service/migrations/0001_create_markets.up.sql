CREATE TABLE markets(
    id UUID PRIMARY KEY,
    symbol VARCHAR(30) NOT NULL UNIQUE,
    base_asset VARCHAR(20) NOT NULL,
    quote_asset VARCHAR(20) NOT NULL,
    tick_size NUMERIC(38,18) NOT NULL,
    lot_size NUMERIC(38,18) NOT NULL,
    min_qty NUMERIC(38,18) NOT NULL,
    max_leverage INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
