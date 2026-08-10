import React, { useState, useEffect, useRef } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useWallet } from '@solana/wallet-adapter-react';
import { createChart, IChartApi, ISeriesApi } from 'lightweight-charts';
import { useThemeStore } from '../store/themeStore';
import { useAuthStore } from '../store/authStore';
import { createTransport } from '../services/transport';
import {
  TrendingUp,
  TrendingDown,
  ArrowLeft,
  Sun,
  Moon,
  RefreshCw,
  X,
  AlertCircle
} from 'lucide-react';

interface Market {
  id: string;
  symbol: string;
  base_asset: string;
  quote_asset: string;
  tick_size: string;
  lot_size: string;
  min_qty: string;
  max_leverage: number;
  status: string;
}

interface Position {
  id: string;
  user_id: string;
  symbol: string;
  side: string;
  size: string;
  entry_price: string;
  leverage: number;
  margin_mode: string;
  margin_locked: string;
  liquidation_price: string;
  realized_pnl: string;
  unrealized_pnl?: string; // Derived or fetched
}

interface Order {
  order_id: string;
  symbol: string;
  side: string;
  order_type: string;
  price: string;
  quantity: string;
  status: string;
  leverage: number;
  trigger_price: string | null;
  reduce_only: boolean;
}

interface TradeHistoryItem {
  id: string;
  symbol: string;
  price: string;
  quantity: string;
  side: string;
  executed_at: string;
}

interface PnlRecord {
  id: string;
  symbol: string;
  closed_size: string;
  realized_pnl: string;
  entry_price: string;
  exit_price: string;
  created_at: string;
}

interface FundingRecord {
  id: string;
  symbol: string;
  side: string;
  size: string;
  funding_rate: string;
  amount: string;
  created_at: string;
}

interface OrderbookLevel {
  price: number;
  size: number;
  total: number;
}

export const TradingPage: React.FC = () => {
  const { symbol } = useParams<{ symbol: string }>();
  const navigate = useNavigate();
  const { theme, toggleTheme } = useThemeStore();
  const { token, userId, pubkey } = useAuthStore();
  
  const activeSymbol = symbol || "BTCUSDT";

  // Market Specs
  const [marketInfo, setMarketInfo] = useState<Market | null>(null);

  // Live Statistics
  const [lastPrice, setLastPrice] = useState<number>(65000);
  const [markPrice, setMarkPrice] = useState<number>(65000);
  const [ticker24hChange, setTicker24hChange] = useState<number>(1.25);
  const [ticker24hHigh, setTicker24hHigh] = useState<number>(66200);
  const [ticker24hLow, setTicker24hLow] = useState<number>(64100);
  const [ticker24hVolume, setTicker24hVolume] = useState<number>(2451080);
  const [fundingCountdown, setFundingCountdown] = useState<string>("00:45:12");

  // Tabs states
  const [bottomTab, setBottomTab] = useState<'positions' | 'orders' | 'history' | 'pnl' | 'funding'>('positions');

  // Account assets states
  const [positions, setPositions] = useState<Position[]>([]);
  const [openOrders, setOpenOrders] = useState<Order[]>([]);
  const [tradesHistory, setTradesHistory] = useState<TradeHistoryItem[]>([]);
  const [pnlHistory, setPnlHistory] = useState<PnlRecord[]>([]);
  const [fundingHistory, setFundingHistory] = useState<FundingRecord[]>([]);
  const [usdcBalance, setUsdcBalance] = useState<string>("0.00");

  // Orderbook States
  const [bids, setBids] = useState<OrderbookLevel[]>([]);
  const [asks, setAsks] = useState<OrderbookLevel[]>([]);
  const [recentTrades, setRecentTrades] = useState<{ price: number; size: number; side: string; time: string }[]>([]);

  // Order Execution States
  const [side, setSide] = useState<'BUY' | 'SELL'>('BUY');
  const [orderType, setOrderType] = useState<string>('LIMIT');
  const [orderPrice, setOrderPrice] = useState<string>("");
  const [orderQty, setOrderQty] = useState<string>("");
  const [triggerPrice, setTriggerPrice] = useState<string>("");
  const [leverage, setLeverage] = useState<number>(10);
  const [marginMode, setMarginMode] = useState<string>('CROSS');
  const [reduceOnly, setReduceOnly] = useState<boolean>(false);
  const [postOnly, setPostOnly] = useState<boolean>(false);
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);
  const [executionMessage, setExecutionMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);

  // Chart Ref
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  const ema9SeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const ema21SeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const [chartResolution, setChartResolution] = useState<string>("1h");

  // Websocket reference
  const transportRef = useRef<any>(null);

  // 1. Fetch Market Specs & Balance
  const fetchMarketSpecs = async () => {
    if (!token || !userId) return;
    try {
      const res = await fetch("http://127.0.0.1:8080/api/v1/markets", {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (res.ok) {
        const list: Market[] = await res.json();
        const spec = list.find(m => m.symbol === activeSymbol);
        if (spec) {
          setMarketInfo(spec);
          setLeverage(Math.min(10, spec.max_leverage));
        }
      }

      const balRes = await fetch(`http://127.0.0.1:8080/api/v1/accounts/${userId}/balance?asset=USDC`, {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (balRes.ok) {
        const balData = await balRes.json();
        setUsdcBalance(balData.available_balance);
      }
    } catch (e) {
      console.error(e);
    }
  };

  // 2. Fetch User Positions, Orders, Fills, Realized PnL & Funding History
  const fetchUserData = async () => {
    if (!token || !userId) return;
    try {
      // Positions
      const posRes = await fetch(`http://127.0.0.1:8080/api/v1/positions/${userId}`, {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (posRes.ok) {
        const list = await posRes.json();
        setPositions(list);
      }

      // Open Orders
      const ordRes = await fetch(`http://127.0.0.1:8080/api/v1/orders/open/${userId}`, {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (ordRes.ok) {
        const list = await ordRes.json();
        setOpenOrders(list);
      }

      // Trades History
      const tradeRes = await fetch(`http://127.0.0.1:8080/api/v1/trades/history/${userId}`, {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (tradeRes.ok) {
        const list = await tradeRes.json();
        setTradesHistory(list);
      }

      // PnL History
      const pnlRes = await fetch(`http://127.0.0.1:8080/api/v1/pnl/history/${userId}`, {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (pnlRes.ok) {
        const list = await pnlRes.json();
        setPnlHistory(list);
      }

      // Funding History
      const fundRes = await fetch(`http://127.0.0.1:8080/api/v1/funding/history/${userId}`, {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (fundRes.ok) {
        const list = await fundRes.json();
        setFundingHistory(list);
      }
    } catch (e) {
      console.error("Failed to fetch user trade panel data:", e);
    }
  };

  // 3. Fetch Historical Candlesticks for Chart
  const fetchCandles = async () => {
    if (!token) return;
    try {
      const res = await fetch(
        `http://127.0.0.1:8080/api/v1/markets/${activeSymbol}/candles?resolution=${chartResolution}`,
        { headers: { "Authorization": `Bearer ${token}` } }
      );
      if (res.ok) {
        const data = await res.json();
        
        const sortedData = data.map((d: any) => ({
          time: Math.floor(new Date(d.time || d.timestamp).getTime() / 1000),
          open: parseFloat(d.open),
          high: parseFloat(d.high),
          low: parseFloat(d.low),
          close: parseFloat(d.close),
          volume: parseFloat(d.volume || "0"),
        })).sort((a: any, b: any) => a.time - b.time);

        if (sortedData.length === 0) return;

        if (candleSeriesRef.current) {
          candleSeriesRef.current.setData(sortedData);
          const last = sortedData[sortedData.length - 1];
          setLastPrice(last.close);
        }

        if (volumeSeriesRef.current) {
          const volData = sortedData.map((d: any) => ({
            time: d.time,
            value: d.volume,
            color: d.close >= d.open ? 'rgba(0, 200, 150, 0.4)' : 'rgba(255, 74, 90, 0.4)',
          }));
          volumeSeriesRef.current.setData(volData);
        }

        if (ema9SeriesRef.current) {
          const ema9Data = calculateEMA(sortedData.map((d: any) => ({ time: d.time, value: d.close })), 9);
          ema9SeriesRef.current.setData(ema9Data);
        }

        if (ema21SeriesRef.current) {
          const ema21Data = calculateEMA(sortedData.map((d: any) => ({ time: d.time, value: d.close })), 21);
          ema21SeriesRef.current.setData(ema21Data);
        }
      }
    } catch (e) {
      console.error(e);
    }
  };

  const calculateEMA = (data: { value: number; time: number }[], period: number) => {
    const emaValues = [];
    if (data.length === 0) return [];
    const k = 2 / (period + 1);
    let emaVal = data[0].value;
    emaValues.push({ time: data[0].time, value: emaVal });
    
    for (let i = 1; i < data.length; i++) {
      emaVal = data[i].value * k + emaVal * (1 - k);
      emaValues.push({ time: data[i].time, value: emaVal });
    }
    return emaValues;
  };

  // Setup Local Chart using Lightweight Charts
  useEffect(() => {
    if (!chartContainerRef.current) return;

    chartContainerRef.current.innerHTML = '';

    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: 400,
      layout: {
        background: { color: theme === 'dark' ? '#090909' : '#ffffff' },
        textColor: theme === 'dark' ? '#d1d5db' : '#1f2937',
      },
      grid: {
        vertLines: { color: theme === 'dark' ? '#202020' : '#f3f4f6' },
        horzLines: { color: theme === 'dark' ? '#202020' : '#f3f4f6' },
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
      },
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: '#00c896',
      downColor: '#ff4a5a',
      borderUpColor: '#00c896',
      borderDownColor: '#ff4a5a',
      wickUpColor: '#00c896',
      wickDownColor: '#ff4a5a',
    });

    const volumeSeries = chart.addHistogramSeries({
      priceFormat: {
        type: 'volume',
      },
      priceScaleId: '',
    });

    volumeSeries.priceScale().applyOptions({
      scaleMargins: {
        top: 0.8,
        bottom: 0,
      },
    });

    const ema9Series = chart.addLineSeries({
      color: '#3a86ff',
      lineWidth: 1.5,
      title: 'EMA 9',
    });

    const ema21Series = chart.addLineSeries({
      color: '#9d4edd',
      lineWidth: 1.5,
      title: 'EMA 21',
    });

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;
    ema9SeriesRef.current = ema9Series;
    ema21SeriesRef.current = ema21Series;

    fetchCandles();

    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
        });
      }
    };
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };
  }, [theme, chartResolution, activeSymbol]);

  // Setup Real-time pubsub connection
  useEffect(() => {
    if (!token) return;

    const transport = createTransport();
    transportRef.current = transport;

    const initConnection = async () => {
      try {
        await transport.connect(token, (msg: any) => {
          // Process websocket messages
          if (msg.symbol && msg.symbol === activeSymbol) {
            // Check if it is a depth update (orderbook)
            if (msg.bids || msg.asks) {
              const formattedBids = (msg.bids || []).map(([price, size]: any, idx: number) => ({
                price: parseFloat(price),
                size: parseFloat(size),
                total: 0
              })).sort((a: any, b: any) => b.price - a.price);

              const formattedAsks = (msg.asks || []).map(([price, size]: any, idx: number) => ({
                price: parseFloat(price),
                size: parseFloat(size),
                total: 0
              })).sort((a: any, b: any) => a.price - b.price);

              // Calculate cumulative totals
              let bidTotal = 0;
              const bidsWithTotals = formattedBids.map((b: any) => {
                bidTotal += b.size;
                return { ...b, total: bidTotal };
              });

              let askTotal = 0;
              const asksWithTotals = formattedAsks.map((a: any) => {
                askTotal += a.size;
                return { ...a, total: askTotal };
              });

              setBids(bidsWithTotals.slice(0, 15));
              setAsks(asksWithTotals.slice(0, 15));
            }

            // Check if it is a trade execute event
            if (msg.maker_order_id || msg.taker_order_id) {
              const execPrice = parseFloat(msg.price);
              const execSize = parseFloat(msg.quantity);
              setLastPrice(execPrice);
              setRecentTrades(prev => [
                {
                  price: execPrice,
                  size: execSize,
                  side: msg.taker_side,
                  time: new Date(msg.executed_at).toLocaleTimeString()
                },
                ...prev.slice(0, 20)
              ]);
            }

            // Check if candle tick updates
            if (msg.open && msg.close) {
              const tick = {
                time: Math.floor(new Date(msg.time || msg.timestamp).getTime() / 1000),
                open: parseFloat(msg.open),
                high: parseFloat(msg.high),
                low: parseFloat(msg.low),
                close: parseFloat(msg.close),
              };
              if (candleSeriesRef.current) {
                candleSeriesRef.current.update(tick);
              }
              if (volumeSeriesRef.current) {
                volumeSeriesRef.current.update({
                  time: tick.time,
                  value: parseFloat(msg.volume || "0"),
                  color: tick.close >= tick.open ? 'rgba(0, 200, 150, 0.4)' : 'rgba(255, 74, 90, 0.4)',
                });
              }
            }
          }

          // Process oracle price ticks
          if (msg.symbol && msg.mark_price) {
            setMarkPrice(parseFloat(msg.mark_price));
          }
        });

        // Subscribe to public symbol channels
        transport.subscribe([
          `orderbook:${activeSymbol}`,
          `trades:${activeSymbol}`,
          `price-ticks`,
          `candles:${activeSymbol}:${chartResolution}`
        ]);

      } catch (e) {
        console.error("Failed to connect websocket transport:", e);
      }
    };

    initConnection();

    return () => {
      transport.disconnect();
    };
  }, [token, activeSymbol, chartResolution]);

  useEffect(() => {
    fetchMarketSpecs();
    fetchUserData();
  }, [token, activeSymbol]);

  // Handle Order Placement
  const handlePlaceOrder = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!token || !userId) return;
    setIsSubmitting(true);
    setExecutionMessage(null);

    const isTriggerOrder = orderType === 'STOP_MARKET' || orderType === 'STOP_LIMIT';

    try {
      const payload = {
        user_id: userId,
        symbol: activeSymbol,
        side,
        order_type: orderType,
        quantity: orderQty,
        price: orderType === 'MARKET' ? null : orderPrice,
        trigger_price: isTriggerOrder ? triggerPrice : null,
        time_in_force: 'GTC',
        leverage: parseInt(String(leverage), 10),
        margin_mode: marginMode,
        reduce_only: reduceOnly,
        post_only: postOnly,
      };

      const res = await fetch("http://127.0.0.1:8080/api/v1/orders", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`
        },
        body: JSON.stringify(payload),
      });

      const data = await res.json();
      if (!res.ok || data.status === 'REJECTED' || data.error_message) {
        throw new Error(data.error_message || "Order placement failed");
      }

      setExecutionMessage({
        type: 'success',
        text: `Order submitted successfully! ID: ${data.order_id}`
      });

      // Clear forms
      setOrderQty("");
      setOrderPrice("");
      setTriggerPrice("");

      // Refresh data
      fetchUserData();
    } catch (err: any) {
      setExecutionMessage({
        type: 'error',
        text: err.message || "Failed to submit order"
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  // Handle Cancel Order
  const handleCancelOrder = async (orderId: string) => {
    if (!token || !userId) return;
    try {
      const res = await fetch("http://127.0.0.1:8080/api/v1/orders/cancel", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`
        },
        body: JSON.stringify({
          user_id: userId,
          order_id: orderId,
          symbol: activeSymbol,
        }),
      });

      if (res.ok) {
        // Refresh open orders list
        fetchUserData();
      }
    } catch (e) {
      console.error(e);
    }
  };

  // Handle Close Position (Places opposite Market order)
  const handleClosePosition = async (pos: Position) => {
    if (!token || !userId) return;
    try {
      const closeSide = pos.side === 'LONG' ? 'SELL' : 'BUY';
      const payload = {
        user_id: userId,
        symbol: pos.symbol,
        side: closeSide,
        order_type: 'MARKET',
        quantity: pos.size,
        price: null,
        trigger_price: null,
        time_in_force: 'GTC',
        leverage: parseInt(String(pos.leverage), 10),
        margin_mode: pos.margin_mode,
        reduce_only: true,
        post_only: false,
      };

      const res = await fetch("http://127.0.0.1:8080/api/v1/orders", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`
        },
        body: JSON.stringify(payload),
      });

      if (res.ok) {
        fetchUserData();
      }
    } catch (e) {
      console.error(e);
    }
  };

  // Spread calculation helper
  const spread = asks.length > 0 && bids.length > 0 ? asks[0].price - bids[0].price : 0;
  const spreadPercentage = bids.length > 0 && spread > 0 ? (spread / bids[0].price) * 100 : 0;

  return (
    <div className="min-h-screen bg-background text-text transition-colors duration-200">
      
      {/* Top Ticker Navigation */}
      <nav className="fixed top-0 left-0 right-0 z-40 flex items-center justify-between px-4 h-14 border-b border-border bg-background/80 backdrop-blur-md">
        <div className="flex items-center space-x-6">
          <button
            onClick={() => navigate('/dashboard')}
            className="flex items-center space-x-1.5 text-xs text-text-secondary hover:text-text transition-all"
          >
            <ArrowLeft size={14} />
            <span>Markets</span>
          </button>

          <div className="h-4 w-px bg-border" />

          {/* Symbol spec info */}
          <div className="flex items-center space-x-3">
            <span className="font-mono font-bold text-sm text-primary">{activeSymbol}</span>
            <span className="text-[10px] bg-primary/20 text-primary border border-primary/25 px-1.5 py-0.5 rounded font-bold uppercase">Perp</span>
          </div>

          <div className="h-4 w-px bg-border hidden lg:block" />

          {/* Price Metrics */}
          <div className="hidden lg:flex items-center space-x-6 font-mono text-xs">
            <div className="flex flex-col">
              <span className="text-[9px] text-text-secondary font-sans font-semibold uppercase">Last Price</span>
              <span className={`font-bold ${ticker24hChange >= 0 ? 'text-success' : 'text-danger'}`}>
                ${lastPrice.toLocaleString(undefined, { minimumFractionDigits: 2 })}
              </span>
            </div>
            <div className="flex flex-col">
              <span className="text-[9px] text-text-secondary font-sans font-semibold uppercase">Mark Price</span>
              <span className="font-bold">${markPrice.toLocaleString(undefined, { minimumFractionDigits: 2 })}</span>
            </div>
            <div className="flex flex-col">
              <span className="text-[9px] text-text-secondary font-sans font-semibold uppercase">24h Change</span>
              <span className={`font-bold ${ticker24hChange >= 0 ? 'text-success' : 'text-danger'}`}>
                {ticker24hChange >= 0 ? '+' : ''}{ticker24hChange}%
              </span>
            </div>
            <div className="flex flex-col">
              <span className="text-[9px] text-text-secondary font-sans font-semibold uppercase">24h High / Low</span>
              <span className="font-semibold text-text-secondary">
                ${ticker24hHigh} / ${ticker24hLow}
              </span>
            </div>
            <div className="flex flex-col">
              <span className="text-[9px] text-text-secondary font-sans font-semibold uppercase">Funding Countdown</span>
              <span className="font-semibold text-primary">{fundingCountdown}</span>
            </div>
          </div>
        </div>

        <div className="flex items-center space-x-3">
          <button
            onClick={toggleTheme}
            className="p-1.5 rounded-lg border border-border bg-card hover:bg-border/30 transition-all"
          >
            {theme === 'dark' ? <Sun size={14} className="text-amber-400" /> : <Moon size={14} className="text-indigo-600" />}
          </button>
        </div>
      </nav>

      {/* Main Grid */}
      <div className="pt-14 grid grid-cols-1 xl:grid-cols-4 min-h-[calc(100vh-56px)] select-none">
        
        {/* Left Widget: Orderbook */}
        <section className="xl:col-span-1 border-r border-border bg-card/15 flex flex-col p-4 space-y-4">
          <div className="flex items-center justify-between border-b border-border/40 pb-2">
            <span className="text-xs font-bold text-text-secondary uppercase">Order Book</span>
            <span className="text-[10px] font-mono text-text-secondary">Spread: ${spread.toFixed(2)} ({spreadPercentage.toFixed(2)}%)</span>
          </div>

          <div className="flex-1 flex flex-col justify-between font-mono text-[11px] leading-tight">
            {/* Asks (Sell Orders) - Red */}
            <div className="flex flex-col-reverse justify-end space-y-0.5 h-[160px] overflow-hidden">
              {asks.map((ask, idx) => (
                <div key={idx} className="flex justify-between hover:bg-danger/5 px-2 py-0.5 rounded relative">
                  <div
                    className="absolute right-0 top-0 bottom-0 bg-danger/5 transition-all pointer-events-none"
                    style={{ width: `${(ask.total / (asks[asks.length - 1]?.total || 1)) * 100}%` }}
                  />
                  <span className="text-danger font-semibold z-10">${ask.price.toFixed(1)}</span>
                  <span className="text-text font-medium z-10">{ask.size.toFixed(3)}</span>
                </div>
              ))}
            </div>

            {/* Mid Spread pricing */}
            <div className="py-2 border-t border-b border-border/40 text-center font-bold text-sm my-2 bg-border/5">
              <span className={ticker24hChange >= 0 ? 'text-success' : 'text-danger'}>
                ${lastPrice.toLocaleString(undefined, { minimumFractionDigits: 1 })}
              </span>
            </div>

            {/* Bids (Buy Orders) - Green */}
            <div className="flex flex-col space-y-0.5 h-[160px] overflow-hidden">
              {bids.map((bid, idx) => (
                <div key={idx} className="flex justify-between hover:bg-success/5 px-2 py-0.5 rounded relative">
                  <div
                    className="absolute right-0 top-0 bottom-0 bg-success/5 transition-all pointer-events-none"
                    style={{ width: `${(bid.total / (bids[bids.length - 1]?.total || 1)) * 100}%` }}
                  />
                  <span className="text-success font-semibold z-10">${bid.price.toFixed(1)}</span>
                  <span className="text-text font-medium z-10">{bid.size.toFixed(3)}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Live Market Trades Feed */}
          <div className="border-t border-border/40 pt-3">
            <div className="flex items-center justify-between mb-2">
              <span className="text-[10px] font-bold text-text-secondary uppercase tracking-wider">Live Trades</span>
              <span className="text-[9px] text-text-secondary font-mono">{activeSymbol}</span>
            </div>

            {/* Header */}
            <div className="grid grid-cols-3 text-[9px] text-text-secondary/60 uppercase font-semibold px-1 mb-1">
              <span>Price</span>
              <span className="text-center">Size</span>
              <span className="text-right">Time</span>
            </div>

            {/* Scrollable trade list */}
            <div className="flex flex-col space-y-0.5 max-h-[200px] overflow-y-auto scrollbar-none">
              {recentTrades.length === 0 ? (
                <div className="text-[10px] text-text-secondary/40 text-center py-4">
                  Waiting for trades...
                </div>
              ) : (
                recentTrades.map((trade, idx) => (
                  <div
                    key={idx}
                    className={`grid grid-cols-3 font-mono text-[10px] px-1 py-0.5 rounded transition-all ${
                      idx === 0 ? 'bg-border/10' : ''
                    }`}
                  >
                    <span className={trade.side === 'BUY' ? 'text-success font-semibold' : 'text-danger font-semibold'}>
                      ${trade.price.toLocaleString(undefined, { minimumFractionDigits: 1 })}
                    </span>
                    <span className="text-text text-center">{trade.size.toFixed(3)}</span>
                    <span className="text-text-secondary text-right text-[9px]">{trade.time}</span>
                  </div>
                ))
              )}
            </div>
          </div>
        </section>

        {/* Center Grid: Chart and Positions Panel */}
        <section className="xl:col-span-2 flex flex-col border-r border-border">
          {/* Top Resolution Selection */}
          <div className="flex items-center justify-between px-6 py-2 border-b border-border bg-card/20">
            <div className="flex items-center space-x-1">
              {["1m", "5m", "15m", "1h", "4h", "1d"].map(res => (
                <button
                  key={res}
                  onClick={() => setChartResolution(res)}
                  className={`px-2.5 py-1 rounded text-xs font-semibold uppercase transition-all ${
                    chartResolution === res ? 'bg-primary text-white' : 'hover:bg-border/20 text-text-secondary'
                  }`}
                >
                  {res}
                </button>
              ))}
            </div>

            <button
              onClick={fetchCandles}
              className="p-1 rounded hover:bg-border/20 transition-all text-text-secondary"
            >
              <RefreshCw size={12} />
            </button>
          </div>

          {/* Lightweight Chart Canvas Container */}
          <div ref={chartContainerRef} className="relative w-full bg-card/10 h-[400px]" />

          {/* Bottom Info Panels */}
          <div className="flex-1 flex flex-col border-t border-border">
            <div className="flex items-center border-b border-border bg-card/10 px-4 h-10 text-xs font-semibold text-text-secondary uppercase overflow-x-auto whitespace-nowrap">
              <button
                onClick={() => setBottomTab('positions')}
                className={`px-4 h-full border-b-2 flex items-center space-x-1 ${bottomTab === 'positions' ? 'border-primary text-text' : 'border-transparent'}`}
              >
                <span>Positions</span>
                <span className="bg-border/40 px-1.5 py-0.5 rounded text-[10px]">{positions.length}</span>
              </button>
              <button
                onClick={() => setBottomTab('orders')}
                className={`px-4 h-full border-b-2 flex items-center space-x-1 ${bottomTab === 'orders' ? 'border-primary text-text' : 'border-transparent'}`}
              >
                <span>Open Orders</span>
                <span className="bg-border/40 px-1.5 py-0.5 rounded text-[10px]">{openOrders.length}</span>
              </button>
              <button
                onClick={() => setBottomTab('history')}
                className={`px-4 h-full border-b-2 ${bottomTab === 'history' ? 'border-primary text-text' : 'border-transparent'}`}
              >
                Trade History
              </button>
              <button
                onClick={() => setBottomTab('pnl')}
                className={`px-4 h-full border-b-2 ${bottomTab === 'pnl' ? 'border-primary text-text' : 'border-transparent'}`}
              >
                Realized PnL
              </button>
              <button
                onClick={() => setBottomTab('funding')}
                className={`px-4 h-full border-b-2 ${bottomTab === 'funding' ? 'border-primary text-text' : 'border-transparent'}`}
              >
                Funding Logs
              </button>
            </div>

            {/* Panel Body details */}
            <div className="flex-1 p-4 overflow-y-auto max-h-[300px]">
              {bottomTab === 'positions' && (
                positions.length === 0 ? (
                  <p className="text-xs text-text-secondary text-center py-8">No open positions found</p>
                ) : (
                  <table className="w-full text-left border-collapse text-[11px] font-mono">
                    <thead>
                      <tr className="border-b border-border/40 text-text-secondary text-[9px] uppercase tracking-wider h-8">
                        <th>Market</th>
                        <th>Side</th>
                        <th>Size</th>
                        <th>Entry Price</th>
                        <th>Liq Price</th>
                        <th>Realized PnL</th>
                        <th className="text-right">Action</th>
                      </tr>
                    </thead>
                    <tbody>
                      {positions.map((pos) => (
                        <tr key={pos.id} className="border-b border-border/20 h-10 hover:bg-border/5">
                          <td className="font-bold text-primary">{pos.symbol}</td>
                          <td className={`font-bold ${pos.side === 'LONG' ? 'text-success' : 'text-danger'}`}>{pos.side}</td>
                          <td>{pos.size}</td>
                          <td>${parseFloat(pos.entry_price).toFixed(2)}</td>
                          <td className="text-danger font-semibold">${parseFloat(pos.liquidation_price).toFixed(2)}</td>
                          <td className={parseFloat(pos.realized_pnl) >= 0 ? 'text-success' : 'text-danger'}>
                            ${parseFloat(pos.realized_pnl).toFixed(2)}
                          </td>
                          <td className="text-right">
                            <button
                              onClick={() => handleClosePosition(pos)}
                              className="px-2.5 py-1 bg-danger hover:bg-danger/95 text-white rounded text-[10px] font-semibold"
                            >
                              Market Close
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )
              )}

              {bottomTab === 'orders' && (
                openOrders.length === 0 ? (
                  <p className="text-xs text-text-secondary text-center py-8">No open orders found</p>
                ) : (
                  <table className="w-full text-left border-collapse text-[11px] font-mono">
                    <thead>
                      <tr className="border-b border-border/40 text-text-secondary text-[9px] uppercase tracking-wider h-8">
                        <th>Symbol</th>
                        <th>Side</th>
                        <th>Type</th>
                        <th>Price</th>
                        <th>Qty</th>
                        <th>Trigger Price</th>
                        <th className="text-right">Action</th>
                      </tr>
                    </thead>
                    <tbody>
                      {openOrders.map((ord) => (
                        <tr key={ord.order_id} className="border-b border-border/20 h-10 hover:bg-border/5">
                          <td className="font-bold text-primary">{ord.symbol}</td>
                          <td className={`font-bold ${ord.side === 'BUY' ? 'text-success' : 'text-danger'}`}>{ord.side}</td>
                          <td>{ord.order_type}</td>
                          <td>${parseFloat(ord.price).toFixed(2)}</td>
                          <td>{ord.quantity}</td>
                          <td className="text-primary font-semibold">
                            {ord.trigger_price ? `$${parseFloat(ord.trigger_price).toFixed(2)}` : 'None'}
                          </td>
                          <td className="text-right">
                            <button
                              onClick={() => handleCancelOrder(ord.order_id)}
                              className="p-1 rounded text-danger hover:bg-danger/10"
                            >
                              <X size={14} />
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )
              )}

              {bottomTab === 'history' && (
                tradesHistory.length === 0 ? (
                  <p className="text-xs text-text-secondary text-center py-8">No trade execution history</p>
                ) : (
                  <table className="w-full text-left border-collapse text-[11px] font-mono">
                    <thead>
                      <tr className="border-b border-border/40 text-text-secondary text-[9px] uppercase tracking-wider h-8">
                        <th>Exec ID</th>
                        <th>Side</th>
                        <th>Price</th>
                        <th>Qty</th>
                        <th className="text-right">Time</th>
                      </tr>
                    </thead>
                    <tbody>
                      {tradesHistory.map((tr) => (
                        <tr key={tr.id} className="border-b border-border/20 h-10">
                          <td className="truncate max-w-[80px] text-text-secondary">{tr.id}</td>
                          <td className={`font-bold ${tr.side === 'BUY' ? 'text-success' : 'text-danger'}`}>{tr.side}</td>
                          <td>${parseFloat(tr.price).toFixed(2)}</td>
                          <td>{tr.quantity}</td>
                          <td className="text-right text-text-secondary">{new Date(tr.executed_at).toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )
              )}

              {bottomTab === 'pnl' && (
                pnlHistory.length === 0 ? (
                  <p className="text-xs text-text-secondary text-center py-8">No realized PnL entries</p>
                ) : (
                  <table className="w-full text-left border-collapse text-[11px] font-mono">
                    <thead>
                      <tr className="border-b border-border/40 text-text-secondary text-[9px] uppercase tracking-wider h-8">
                        <th>Symbol</th>
                        <th>Closed Size</th>
                        <th>Realized PnL</th>
                        <th>Entry Price</th>
                        <th>Exit Price</th>
                        <th className="text-right">Time</th>
                      </tr>
                    </thead>
                    <tbody>
                      {pnlHistory.map((pnl) => (
                        <tr key={pnl.id} className="border-b border-border/20 h-10">
                          <td className="font-bold text-primary">{pnl.symbol}</td>
                          <td>{pnl.closed_size}</td>
                          <td className={`font-bold ${parseFloat(pnl.realized_pnl) >= 0 ? 'text-success' : 'text-danger'}`}>
                            ${parseFloat(pnl.realized_pnl).toFixed(2)}
                          </td>
                          <td>${parseFloat(pnl.entry_price).toFixed(2)}</td>
                          <td>${parseFloat(pnl.exit_price).toFixed(2)}</td>
                          <td className="text-right text-text-secondary">{new Date(pnl.created_at).toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )
              )}

              {bottomTab === 'funding' && (
                fundingHistory.length === 0 ? (
                  <p className="text-xs text-text-secondary text-center py-8">No funding fee settlements</p>
                ) : (
                  <table className="w-full text-left border-collapse text-[11px] font-mono">
                    <thead>
                      <tr className="border-b border-border/40 text-text-secondary text-[9px] uppercase tracking-wider h-8">
                        <th>Symbol</th>
                        <th>Position Size</th>
                        <th>Funding Rate</th>
                        <th>Adjustment</th>
                        <th className="text-right">Time</th>
                      </tr>
                    </thead>
                    <tbody>
                      {fundingHistory.map((f) => (
                        <tr key={f.id} className="border-b border-border/20 h-10">
                          <td className="font-bold text-primary">{f.symbol}</td>
                          <td>{f.size} ({f.side})</td>
                          <td className="text-primary font-semibold">{(parseFloat(f.funding_rate) * 100).toFixed(4)}%</td>
                          <td className={`font-bold ${parseFloat(f.amount) >= 0 ? 'text-success' : 'text-danger'}`}>
                            ${parseFloat(f.amount).toFixed(2)}
                          </td>
                          <td className="text-right text-text-secondary">{new Date(f.created_at).toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )
              )}
            </div>
          </div>
        </section>

        {/* Right Widget: Order Placement & Inputs */}
        <section className="xl:col-span-1 p-4 bg-card/25 flex flex-col space-y-6">
          <div className="space-y-1">
            <span className="text-[10px] font-bold text-text-secondary uppercase">Available Balance</span>
            <p className="text-xl font-bold font-mono text-text">${parseFloat(usdcBalance).toLocaleString()}</p>
          </div>

          <form onSubmit={handlePlaceOrder} className="space-y-4 flex-1">
            
            {/* Side Tabs (Buy Long / Sell Short) */}
            <div className="flex bg-background border border-border rounded-xl p-1 h-11">
              <button
                type="button"
                onClick={() => setSide('BUY')}
                className={`flex-1 rounded-lg text-xs font-bold transition-all ${side === 'BUY' ? 'bg-success text-white' : 'text-text-secondary'}`}
              >
                Buy / Long
              </button>
              <button
                type="button"
                onClick={() => setSide('SELL')}
                className={`flex-1 rounded-lg text-xs font-bold transition-all ${side === 'SELL' ? 'bg-danger text-white' : 'text-text-secondary'}`}
              >
                Sell / Short
              </button>
            </div>

            {/* Order Type selectors */}
            <div className="grid grid-cols-2 gap-2">
              <div className="flex flex-col space-y-1">
                <label className="text-[9px] font-bold text-text-secondary uppercase">Type</label>
                <select
                  value={orderType}
                  onChange={(e) => setOrderType(e.target.value)}
                  className="h-9 px-3 rounded-lg border border-border bg-background text-xs focus:outline-none focus:border-primary/50 font-semibold"
                >
                  <option value="LIMIT">Limit</option>
                  <option value="MARKET">Market</option>
                  <option value="STOP_LIMIT">Stop Limit</option>
                  <option value="STOP_MARKET">Stop Market</option>
                </select>
              </div>

              <div className="flex flex-col space-y-1">
                <label className="text-[9px] font-bold text-text-secondary uppercase">Margin Mode</label>
                <select
                  value={marginMode}
                  onChange={(e) => setMarginMode(e.target.value)}
                  className="h-9 px-3 rounded-lg border border-border bg-background text-xs focus:outline-none focus:border-primary/50 font-semibold"
                >
                  <option value="CROSS">Cross</option>
                  <option value="ISOLATED">Isolated</option>
                </select>
              </div>
            </div>

            {/* Dynamic Inputs depending on type */}
            {orderType !== 'MARKET' && (
              <div className="flex flex-col space-y-1">
                <label className="text-[9px] font-bold text-text-secondary uppercase">Limit Price (USDC)</label>
                <input
                  type="number"
                  step="0.01"
                  required
                  placeholder="Price"
                  value={orderPrice}
                  onChange={(e) => setOrderPrice(e.target.value)}
                  className="h-10 px-4 rounded-lg border border-border bg-background text-xs focus:outline-none focus:border-primary/50 font-mono font-semibold"
                />
              </div>
            )}

            {(orderType === 'STOP_LIMIT' || orderType === 'STOP_MARKET') && (
              <div className="flex flex-col space-y-1 bg-primary/5 border border-primary/10 p-3 rounded-xl">
                <label className="text-[9px] font-bold text-text-secondary uppercase">Trigger Price (USDC)</label>
                <input
                  type="number"
                  step="0.01"
                  required
                  placeholder="Trigger Price"
                  value={triggerPrice}
                  onChange={(e) => setTriggerPrice(e.target.value)}
                  className="h-10 px-4 rounded-lg border border-border bg-background text-xs focus:outline-none focus:border-primary/50 font-mono font-semibold"
                />
                <span className="text-[8px] text-text-secondary leading-relaxed pt-1 flex items-start space-x-1">
                  <AlertCircle size={10} className="mt-0.5 flex-shrink-0" />
                  <span>Conditional order executes once mark price crosses trigger price bounds.</span>
                </span>
              </div>
            )}

            <div className="flex flex-col space-y-1">
              <label className="text-[9px] font-bold text-text-secondary uppercase">Quantity ({marketInfo?.base_asset || "Size"})</label>
              <input
                type="number"
                step="0.001"
                required
                placeholder="0.00"
                value={orderQty}
                onChange={(e) => setOrderQty(e.target.value)}
                className="h-10 px-4 rounded-lg border border-border bg-background text-xs focus:outline-none focus:border-primary/50 font-mono font-semibold"
              />
            </div>

            {/* Leverage Slider */}
            <div className="flex flex-col space-y-2 pt-2">
              <div className="flex justify-between text-[10px] font-bold text-text-secondary">
                <span>LEVERAGE</span>
                <span className="text-primary font-mono">{leverage}x</span>
              </div>
              <input
                type="range"
                min="1"
                max={marketInfo?.max_leverage || 20}
                value={leverage}
                onChange={(e) => setLeverage(parseInt(e.target.value))}
                className="w-full h-1.5 bg-border rounded-lg appearance-none cursor-pointer accent-primary"
              />
              <div className="flex justify-between text-[8px] font-mono text-text-secondary">
                <span>1x</span>
                <span>{marketInfo?.max_leverage || 20}x Max</span>
              </div>
            </div>

            {/* Advanced Checkboxes */}
            <div className="flex flex-col space-y-2 pt-2 text-[10px] text-text-secondary font-semibold">
              <label className="flex items-center space-x-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={reduceOnly}
                  onChange={(e) => setReduceOnly(e.target.checked)}
                  className="rounded border-border text-primary focus:ring-primary/20"
                />
                <span>Reduce Only</span>
              </label>

              {orderType !== 'MARKET' && (
                <label className="flex items-center space-x-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={postOnly}
                    onChange={(e) => setPostOnly(e.target.checked)}
                    className="rounded border-border text-primary focus:ring-primary/20"
                  />
                  <span>Post Only</span>
                </label>
              )}
            </div>

            {executionMessage && (
              <p className={`text-[10px] font-semibold leading-relaxed p-2 rounded ${
                executionMessage.type === 'success' ? 'bg-success/5 border border-success/15 text-success' : 'bg-danger/5 border border-danger/15 text-danger'
              }`}>
                {executionMessage.text}
              </p>
            )}

            <button
              type="submit"
              disabled={isSubmitting}
              className={`w-full h-12 rounded-xl text-xs font-bold text-white transition-all shadow-md ${
                side === 'BUY' ? 'bg-success hover:bg-success/95 shadow-success/10' : 'bg-danger hover:bg-danger/95 shadow-danger/10'
              }`}
            >
              {isSubmitting ? "Submitting..." : `${side === 'BUY' ? 'Buy / Long' : 'Sell / Short'} ${activeSymbol}`}
            </button>
          </form>
        </section>
      </div>
    </div>
  );
};
