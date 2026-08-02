
INSERT INTO accounts (id, user_id, asset, balance, frozen)
VALUES (
    '00000000-0000-0000-0000-000000000000',
    '00000000-0000-0000-0000-000000000000', 
    'USDT',
    1000000.0,                             
    0.0
) ON CONFLICT DO NOTHING;
