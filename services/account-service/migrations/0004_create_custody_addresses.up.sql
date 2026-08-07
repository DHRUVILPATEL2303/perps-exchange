CREATE TABLE IF NOT EXISTS custody_addresses(
    user_id UUID PRIMARY KEY,
    pda_address VARCHAR(44) NOT NULL,
    usdc_ata VARCHAR(44) NOT NULL,
    usdt_ata VARCHAR(44) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_custody_usdc_ata ON custody_addresses(usdc_ata);
CREATE UNIQUE INDEX IF NOT EXISTS idx_custody_usdt_ata ON custody_addresses(usdt_ata);
