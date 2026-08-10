-- candles_3m
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_3m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('3 minutes', time) AS bucket,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(quantity) AS volume
FROM trades
GROUP BY bucket, symbol
WITH NO DATA;

-- candles_15m
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_15m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', time) AS bucket,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(quantity) AS volume
FROM trades
GROUP BY bucket, symbol
WITH NO DATA;

-- candles_30m
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_30m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('30 minutes', time) AS bucket,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(quantity) AS volume
FROM trades
GROUP BY bucket, symbol
WITH NO DATA;

-- candles_2h
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_2h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('2 hours', time) AS bucket,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(quantity) AS volume
FROM trades
GROUP BY bucket, symbol
WITH NO DATA;

-- candles_4h
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_4h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('4 hours', time) AS bucket,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(quantity) AS volume
FROM trades
GROUP BY bucket, symbol
WITH NO DATA;

-- candles_6h
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_6h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('6 hours', time) AS bucket,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(quantity) AS volume
FROM trades
GROUP BY bucket, symbol
WITH NO DATA;

-- candles_8h
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_8h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('8 hours', time) AS bucket,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(quantity) AS volume
FROM trades
GROUP BY bucket, symbol
WITH NO DATA;

-- candles_12h
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_12h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('12 hours', time) AS bucket,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(quantity) AS volume
FROM trades
GROUP BY bucket, symbol
WITH NO DATA;

-- candles_1d
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS bucket,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(quantity) AS volume
FROM trades
GROUP BY bucket, symbol
WITH NO DATA;

-- candles_3d
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_3d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('3 days', time) AS bucket,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(quantity) AS volume
FROM trades
GROUP BY bucket, symbol
WITH NO DATA;

-- candles_1w
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1w
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 week', time) AS bucket,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(quantity) AS volume
FROM trades
GROUP BY bucket, symbol
WITH NO DATA;

-- candles_1M
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1M
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('30 days', time) AS bucket,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(quantity) AS volume
FROM trades
GROUP BY bucket, symbol
WITH NO DATA;


-- Continuous aggregate policies
SELECT add_continuous_aggregate_policy('candles_3m',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '3 minutes',
    schedule_interval => INTERVAL '20 seconds');

SELECT add_continuous_aggregate_policy('candles_15m',
    start_offset => INTERVAL '15 hours',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '1 minute');

SELECT add_continuous_aggregate_policy('candles_30m',
    start_offset => INTERVAL '30 hours',
    end_offset => INTERVAL '30 minutes',
    schedule_interval => INTERVAL '2 minutes');

SELECT add_continuous_aggregate_policy('candles_2h',
    start_offset => INTERVAL '2 days',
    end_offset => INTERVAL '2 hours',
    schedule_interval => INTERVAL '10 minutes');

SELECT add_continuous_aggregate_policy('candles_4h',
    start_offset => INTERVAL '4 days',
    end_offset => INTERVAL '4 hours',
    schedule_interval => INTERVAL '15 minutes');

SELECT add_continuous_aggregate_policy('candles_6h',
    start_offset => INTERVAL '6 days',
    end_offset => INTERVAL '6 hours',
    schedule_interval => INTERVAL '20 minutes');

SELECT add_continuous_aggregate_policy('candles_8h',
    start_offset => INTERVAL '8 days',
    end_offset => INTERVAL '8 hours',
    schedule_interval => INTERVAL '30 minutes');

SELECT add_continuous_aggregate_policy('candles_12h',
    start_offset => INTERVAL '12 days',
    end_offset => INTERVAL '12 hours',
    schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('candles_1d',
    start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '2 hours');

SELECT add_continuous_aggregate_policy('candles_3d',
    start_offset => INTERVAL '90 days',
    end_offset => INTERVAL '3 days',
    schedule_interval => INTERVAL '6 hours');

SELECT add_continuous_aggregate_policy('candles_1w',
    start_offset => INTERVAL '180 days',
    end_offset => INTERVAL '1 week',
    schedule_interval => INTERVAL '12 hours');

SELECT add_continuous_aggregate_policy('candles_1M',
    start_offset => INTERVAL '720 days',
    end_offset => INTERVAL '30 days',
    schedule_interval => INTERVAL '24 hours');
