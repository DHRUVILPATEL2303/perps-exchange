#!/bin/bash

for pidfile in logs/*.pid; do
    if [ -f "$pidfile" ]; then
        pid=$(cat "$pidfile")
        kill "$pid" 2>/dev/null
        rm "$pidfile"
    fi
done

killall market-service account-service trading-service risk-engine-service matching-engine oracle-aggregator binance-liquidation api-gateway blockchain-listener 2>/dev/null
