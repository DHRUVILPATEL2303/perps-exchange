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

RUN cargo build --release \
    -p market-service \
    -p account-service \
    -p trading-service \
    -p risk-engine-service \
    -p matching-engine \
    -p oracle-aggregator \
    -p binance-liquidation \
    -p api-gateway \
    -p chart-service

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    openssl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/perps-exchange/target/release/market-service /usr/local/bin/
COPY --from=builder /usr/src/perps-exchange/target/release/account-service /usr/local/bin/
COPY --from=builder /usr/src/perps-exchange/target/release/trading-service /usr/local/bin/
COPY --from=builder /usr/src/perps-exchange/target/release/risk-engine-service /usr/local/bin/
COPY --from=builder /usr/src/perps-exchange/target/release/matching-engine /usr/local/bin/
COPY --from=builder /usr/src/perps-exchange/target/release/oracle-aggregator /usr/local/bin/
COPY --from=builder /usr/src/perps-exchange/target/release/binance-liquidation /usr/local/bin/
COPY --from=builder /usr/src/perps-exchange/target/release/api-gateway /usr/local/bin/
COPY --from=builder /usr/src/perps-exchange/target/release/chart-service /usr/local/bin/

COPY --from=builder /usr/src/perps-exchange/configs /app/configs
COPY --from=builder /usr/src/perps-exchange/configs/common.docker.toml /app/configs/common.toml
