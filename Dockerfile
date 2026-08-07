FROM rust:1.80-slim AS builder

WORKDIR /usr/src/perps-exchange

ENV RUSTUP_TOOLCHAIN_PROFILE=minimal

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    cmake \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/perps-exchange/target \
    cargo build --release \
    -p market-service \
    -p account-service \
    -p trading-service \
    -p risk-engine-service \
    -p matching-engine \
    -p oracle-aggregator \
    -p binance-liquidation \
    -p api-gateway \
    -p chart-service \
    -p telegram-bot \
    -p blockchain-listener \
    && mkdir -p /tmp/bins \
    && cp target/release/market-service /tmp/bins/ \
    && cp target/release/account-service /tmp/bins/ \
    && cp target/release/trading-service /tmp/bins/ \
    && cp target/release/risk-engine-service /tmp/bins/ \
    && cp target/release/matching-engine /tmp/bins/ \
    && cp target/release/oracle-aggregator /tmp/bins/ \
    && cp target/release/binance-liquidation /tmp/bins/ \
    && cp target/release/api-gateway /tmp/bins/ \
    && cp target/release/chart-service /tmp/bins/ \
    && cp target/release/telegram-bot /tmp/bins/ \
    && cp target/release/blockchain-listener /tmp/bins/

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    openssl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /tmp/bins/market-service /usr/local/bin/
COPY --from=builder /tmp/bins/account-service /usr/local/bin/
COPY --from=builder /tmp/bins/trading-service /usr/local/bin/
COPY --from=builder /tmp/bins/risk-engine-service /usr/local/bin/
COPY --from=builder /tmp/bins/matching-engine /usr/local/bin/
COPY --from=builder /tmp/bins/oracle-aggregator /usr/local/bin/
COPY --from=builder /tmp/bins/binance-liquidation /usr/local/bin/
COPY --from=builder /tmp/bins/api-gateway /usr/local/bin/
COPY --from=builder /tmp/bins/chart-service /usr/local/bin/
COPY --from=builder /tmp/bins/telegram-bot /usr/local/bin/
COPY --from=builder /tmp/bins/blockchain-listener /usr/local/bin/

COPY --from=builder /usr/src/perps-exchange/configs /app/configs
COPY --from=builder /usr/src/perps-exchange/configs/common.docker.toml /app/configs/common.toml
