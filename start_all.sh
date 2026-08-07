#!/bin/bash

docker-compose -f docker/docker-compose.yaml up -d

cargo build

mkdir -p logs

target/debug/market-service > logs/market-service.log 2>&1 &
echo $! > logs/market-service.pid

target/debug/account-service > logs/account-service.log 2>&1 &
echo $! > logs/account-service.pid

sleep 1

target/debug/trading-service > logs/trading-service.log 2>&1 &
echo $! > logs/trading-service.pid

target/debug/risk-engine-service > logs/risk-engine-service.log 2>&1 &
echo $! > logs/risk-engine-service.pid

target/debug/matching-engine > logs/matching-engine.log 2>&1 &
echo $! > logs/matching-engine.pid

target/debug/oracle-aggregator > logs/oracle-aggregator.log 2>&1 &
echo $! > logs/oracle-aggregator.pid

target/debug/binance-liquidation > logs/binance-liquidation.log 2>&1 &
echo $! > logs/binance-liquidation.pid

target/debug/api-gateway > logs/api-gateway.log 2>&1 &
echo $! > logs/api-gateway.pid

target/debug/blockchain-listener > logs/blockchain-listener.log 2>&1 &
echo $! > logs/blockchain-listener.pid
