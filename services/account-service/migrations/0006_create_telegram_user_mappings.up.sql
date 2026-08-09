CREATE TABLE IF NOT EXISTS telegram_user_mappings (
    user_id UUID PRIMARY KEY,
    telegram_chat_id BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
