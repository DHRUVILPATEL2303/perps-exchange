import React, { useState, useEffect } from 'react';
import { useWallet } from '@solana/wallet-adapter-react';
import { useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import { useThemeStore } from '../store/themeStore';
import { useAuthStore } from '../store/authStore';
import { TrendingUp, Search, ArrowDownLeft, ArrowUpRight, User, Bell, Sun, Moon, LogOut, RefreshCw, Copy, CircleCheck as CheckCircle, ExternalLink, ChevronRight, TrendingDown, DollarSign } from 'lucide-react';

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

interface Balance {
  available_balance: string;
  locked_balance: string;
}

export const Dashboard: React.FC = () => {
  const { theme, toggleTheme } = useThemeStore();
  const { token, userId, pubkey, clearAuth } = useAuthStore();
  const { disconnect } = useWallet();
  const navigate = useNavigate();

  const [markets, setMarkets] = useState<Market[]>([]);
  const [balance, setBalance] = useState<Balance>({ available_balance: "0.00", locked_balance: "0.00" });
  const [searchQuery, setSearchQuery] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);

  // Deposit/Withdraw Modal States
  const [showDepositModal, setShowDepositModal] = useState(false);
  const [showWithdrawModal, setShowWithdrawModal] = useState(false);
  const [depositAddress, setDepositAddress] = useState<string>("");
  const [isFetchingDepositAddr, setIsFetchingDepositAddr] = useState(false);
  const [copied, setCopied] = useState(false);

  // Withdraw Form States
  const [withdrawAmount, setWithdrawAmount] = useState("");
  const [withdrawDest, setWithdrawDest] = useState(pubkey || "");
  const [isWithdrawing, setIsWithdrawing] = useState(false);
  const [withdrawTx, setWithdrawTx] = useState<string | null>(null);
  const [withdrawError, setWithdrawError] = useState<string | null>(null);

  const fetchMarketsAndBalances = async () => {
    if (!token || !userId) return;
    setIsRefreshing(true);
    try {
      // 1. Fetch Markets list
      const marketsRes = await fetch("http://127.0.0.1:8080/api/v1/markets", {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (marketsRes.ok) {
        const data = await marketsRes.json();
        setMarkets(data);
      }

      // 2. Fetch User balance
      const balanceRes = await fetch(`http://127.0.0.1:8080/api/v1/accounts/${userId}/balance?asset=USDC`, {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (balanceRes.ok) {
        const data = await balanceRes.json();
        setBalance(data);
      }
    } catch (e) {
      console.error("Failed to fetch dashboard data:", e);
    } finally {
      setIsLoading(false);
      setIsRefreshing(false);
    }
  };

  useEffect(() => {
    fetchMarketsAndBalances();
  }, [token, userId]);

  // Derive deposit address from API Gateway
  const handleOpenDeposit = async () => {
    setShowDepositModal(true);
    if (depositAddress || !token || !userId) return;
    setIsFetchingDepositAddr(true);
    try {
      const res = await fetch(`http://127.0.0.1:8080/api/v1/accounts/${userId}/deposit-address`, {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (res.ok) {
        const data = await res.json();
        setDepositAddress(data.deposit_address || data.pda_address || "");
      }
    } catch (e) {
      console.error(e);
    } finally {
      setIsFetchingDepositAddr(false);
    }
  };

  // Execute withdrawal
  const handleWithdraw = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!token || !userId || !withdrawAmount || !withdrawDest) return;
    setIsWithdrawing(true);
    setWithdrawError(null);
    setWithdrawTx(null);
    try {
      const res = await fetch("http://127.0.0.1:8080/api/v1/accounts/withdraw", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`
        },
        body: JSON.stringify({
          user_id: userId,
          amount: withdrawAmount,
          asset: "USDC",
          destination_address: withdrawDest,
        }),
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || "Withdrawal failed");
      }
      const data = await res.json();
      setWithdrawTx(data.tx_hash);
      setWithdrawAmount("");
      // Refresh balance
      fetchMarketsAndBalances();
    } catch (err: any) {
      setWithdrawError(err.message || "Withdrawal failed");
    } finally {
      setIsWithdrawing(false);
    }
  };

  const handleCopy = () => {
    navigator.clipboard.writeText(depositAddress);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleLogout = () => {
    disconnect();
    clearAuth();
    navigate('/');
  };

  const filteredMarkets = markets.filter(m =>
    m.symbol.toLowerCase().includes(searchQuery.toLowerCase())
  );

  // Total balance computation
  const totalBalance = parseFloat(balance.available_balance) + parseFloat(balance.locked_balance);

  return (
    <div className="exchange-shell min-h-screen bg-background text-text transition-colors duration-200">
      {/* Header bar */}
      <header className="fixed inset-x-0 top-0 z-40 flex h-16 items-center justify-between border-b border-border bg-background/85 px-6 backdrop-blur-xl md:px-10">
        <div className="flex items-center space-x-6">
          <div className="flex items-center space-x-2 cursor-pointer" onClick={() => navigate('/')}>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M12 2L2 22h20L12 2zm0 4l6.5 13h-13L12 6z" fill="var(--primary)" />
            </svg>
            <span className="font-sans font-extrabold text-base tracking-tight">dpkv perps</span>
          </div>

          <div className="flex items-center space-x-1 text-xs font-semibold text-text-secondary">
            <button className="px-3 py-1.5 rounded-lg text-text bg-border/20">Dashboard</button>
            <button onClick={() => navigate('/profile')} className="px-3 py-1.5 rounded-lg hover:text-text hover:bg-border/10 transition-all">Profile</button>
          </div>
        </div>

        {/* Global Search */}
        <div className="hidden md:flex items-center w-80 relative">
          <Search size={16} className="absolute left-3 text-text-secondary" />
          <input
            type="text"
            placeholder="Search markets..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full h-8 pl-9 pr-4 rounded-lg border border-border bg-card/50 text-xs focus:outline-none focus:border-primary/50 placeholder:text-text-secondary"
          />
        </div>

        <div className="flex items-center space-x-4">
          {/* Quick Balance indicator */}
          <div className="hidden sm:flex flex-col text-right">
            <span className="text-[10px] text-text-secondary font-semibold uppercase tracking-wider">Account Equity</span>
            <span className="text-xs font-mono font-bold">${totalBalance.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
          </div>

          <button
            onClick={toggleTheme}
            className="p-1.5 rounded-lg border border-border bg-card hover:bg-border/30 transition-all"
            aria-label="Toggle theme"
          >
            {theme === 'dark' ? <Sun size={14} className="text-amber-400" /> : <Moon size={14} className="text-indigo-600" />}
          </button>

          <button
            onClick={() => navigate('/profile')}
            className="p-1.5 rounded-lg border border-border bg-card hover:bg-border/30 text-text-secondary hover:text-text transition-all"
            aria-label="Profile"
          >
            <User size={14} />
          </button>

          <button
            onClick={handleLogout}
            className="p-1.5 rounded-lg border border-border bg-card hover:bg-danger/10 hover:text-danger hover:border-danger/20 transition-all"
            aria-label="Logout"
          >
            <LogOut size={14} />
          </button>
        </div>
      </header>

      {/* Main Container */}
      <main className="mx-auto grid max-w-[1440px] grid-cols-1 gap-5 px-4 pb-10 pt-24 md:px-8 lg:grid-cols-3">
        
        {/* Left Column: Markets list */}
        <section className="space-y-5 lg:col-span-2">
          <div className="flex items-center justify-between">
            <div className="space-y-1">
              <h2 className="text-xl font-extrabold tracking-tight">Perpetual Markets</h2>
              <p className="text-xs text-text-secondary">Liquid futures contracts settled instantly in USDC</p>
            </div>

            <button
              onClick={fetchMarketsAndBalances}
              className={`p-2 rounded-lg border border-border hover:bg-border/20 transition-all ${isRefreshing ? 'animate-spin' : ''}`}
            >
              <RefreshCw size={14} />
            </button>
          </div>

          {/* Search bar for small screens */}
          <div className="flex md:hidden items-center w-full relative">
            <Search size={16} className="absolute left-3 text-text-secondary" />
            <input
              type="text"
              placeholder="Search markets..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full h-9 pl-9 pr-4 rounded-lg border border-border bg-card/50 text-xs focus:outline-none focus:border-primary/50"
            />
          </div>

          {isLoading ? (
            // Skeleton Loader
            <div className="space-y-3">
              {[1, 2, 3].map(i => (
                <div key={i} className="h-16 w-full bg-card border border-border rounded-xl animate-pulse" />
              ))}
            </div>
          ) : filteredMarkets.length === 0 ? (
            <div className="p-12 text-center border border-border rounded-xl bg-card/35 space-y-2">
              <p className="text-sm font-semibold text-text-secondary">No markets found</p>
              <p className="text-xs text-text-secondary">Try searching for another contract symbol or check back later.</p>
            </div>
          ) : (
            <div className="exchange-panel overflow-hidden rounded-xl bg-card">
              <div className="overflow-x-auto">
                <table className="w-full text-left border-collapse">
                  <thead>
                    <tr className="border-b border-border text-[10px] font-bold text-text-secondary uppercase tracking-wider bg-card/50 h-10 px-4">
                      <th className="py-2 pl-4">Market</th>
                      <th className="py-2">Base Asset</th>
                      <th className="py-2">Quote Asset</th>
                      <th className="py-2 text-right">Tick Size</th>
                      <th className="py-2 text-right">Min Qty</th>
                      <th className="py-2 text-right pr-4">Leverage</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredMarkets.map((market) => (
                      <tr
                        key={market.id}
                        onClick={() => navigate(`/markets/${market.symbol}`)}
                        className="market-grid-row h-[72px] cursor-pointer border-b border-border/40 transition-all duration-150"
                      >
                        <td className="py-2 pl-4 font-mono font-bold text-sm text-primary flex items-center space-x-2 h-full">
                          <span>{market.symbol}</span>
                          <span className="text-[10px] bg-primary/10 text-primary border border-primary/20 px-1.5 py-0.5 rounded-md font-sans font-semibold uppercase">Perp</span>
                        </td>
                        <td className="py-2 text-xs font-semibold">{market.base_asset}</td>
                        <td className="py-2 text-xs font-semibold">{market.quote_asset}</td>
                        <td className="py-2 text-xs font-mono font-semibold text-right">{market.tick_size}</td>
                        <td className="py-2 text-xs font-mono font-semibold text-right">{market.min_qty}</td>
                        <td className="py-2 text-xs font-mono font-semibold text-right pr-4 text-text-secondary">
                          {market.max_leverage}x
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </section>

        {/* Right Column: Account Balance Card & Overview */}
        <section className="space-y-5">
          <div className="exchange-panel relative overflow-hidden rounded-xl bg-card p-6 shadow-sm">
            <div className="absolute top-0 right-0 translate-x-1/4 -translate-y-1/4 w-32 h-32 bg-primary/10 rounded-full blur-2xl pointer-events-none" />
            
            <div className="space-y-2">
              <span className="text-[10px] font-bold text-text-secondary uppercase tracking-wider">Exchange Balance (USDC)</span>
              <h3 className="text-3xl font-extrabold font-mono tracking-tight text-text">
                ${totalBalance.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
              </h3>
            </div>

            <div className="grid grid-cols-2 gap-4 border-t border-b border-border/40 py-4 font-mono text-xs">
              <div className="space-y-1">
                <span className="text-[10px] font-semibold text-text-secondary font-sans uppercase">Available</span>
                <p className="font-bold">${parseFloat(balance.available_balance).toLocaleString(undefined, { minimumFractionDigits: 2 })}</p>
              </div>
              <div className="space-y-1 text-right">
                <span className="text-[10px] font-semibold text-text-secondary font-sans uppercase">Locked Margin</span>
                <p className="font-bold">${parseFloat(balance.locked_balance).toLocaleString(undefined, { minimumFractionDigits: 2 })}</p>
              </div>
            </div>

            <div className="flex gap-4">
              <button
                onClick={handleOpenDeposit}
                className="flex-1 flex items-center justify-center space-x-2 h-10 bg-primary hover:bg-primary/95 text-white rounded-lg text-xs font-semibold transition-all shadow-md shadow-primary/20"
              >
                <ArrowDownLeft size={14} />
                <span>Deposit</span>
              </button>
              <button
                onClick={() => setShowWithdrawModal(true)}
                className="flex-1 flex items-center justify-center space-x-2 h-10 border border-border bg-card hover:bg-border/30 rounded-lg text-xs font-semibold transition-all"
              >
                <ArrowUpRight size={14} />
                <span>Withdraw</span>
              </button>
            </div>
          </div>

          {/* Wallet Info Widget */}
          <div className="exchange-panel rounded-xl bg-card p-6">
            <h4 className="text-xs font-bold text-text-secondary uppercase tracking-wider">Solana Connected Wallet</h4>
            <div className="flex items-center justify-between border border-border/50 bg-background/50 rounded-xl px-4 py-3">
              <div className="flex flex-col space-y-0.5">
                <span className="text-[10px] text-text-secondary font-semibold uppercase">Wallet Address</span>
                <span className="text-xs font-mono font-bold tracking-tight">
                  {pubkey ? `${pubkey.slice(0, 8)}...${pubkey.slice(-8)}` : ''}
                </span>
              </div>
              <button
                onClick={() => pubkey && navigator.clipboard.writeText(pubkey)}
                className="p-1.5 rounded-lg border border-border/50 hover:bg-border/30 text-text-secondary hover:text-text transition-all"
              >
                <Copy size={12} />
              </button>
            </div>
          </div>
        </section>
      </main>

      {/* DEPOSIT MODAL */}
      <AnimatePresence>
        {showDepositModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center p-6 bg-black/60 backdrop-blur-sm">
            <motion.div
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="w-full max-w-md p-6 rounded-2xl border border-border bg-card space-y-6 shadow-2xl relative"
            >
              <div className="space-y-1">
                <h3 className="text-lg font-bold">Deposit Funds</h3>
                <p className="text-xs text-text-secondary">Send USDC (Solana Devnet) to your account Program PDA</p>
              </div>

              {isFetchingDepositAddr ? (
                <div className="h-16 w-full bg-border/20 rounded-xl animate-pulse" />
              ) : (
                <div className="space-y-4">
                  <div className="flex items-center justify-between border border-border bg-background rounded-xl px-4 py-3">
                    <div className="flex flex-col space-y-0.5 max-w-[80%]">
                      <span className="text-[10px] text-text-secondary font-semibold uppercase">Program PDA Address</span>
                      <span className="text-xs font-mono font-bold truncate">{depositAddress}</span>
                    </div>
                    <button
                      onClick={handleCopy}
                      className="p-2 rounded-lg border border-border/60 hover:bg-border/30 text-text-secondary hover:text-text transition-all"
                    >
                      {copied ? <CheckCircle size={14} className="text-success" /> : <Copy size={14} />}
                    </button>
                  </div>
                  <p className="text-[10px] text-text-secondary leading-relaxed bg-primary/5 border border-primary/10 p-3 rounded-lg">
                    ⚠️ Deposits will credit automatically once the transaction is broadcast and validated by our program listener. Only send Devnet USDC.
                  </p>
                </div>
              )}

              <button
                onClick={() => setShowDepositModal(false)}
                className="w-full h-10 border border-border bg-background hover:bg-border/30 rounded-lg text-xs font-semibold transition-all"
              >
                Close
              </button>
            </motion.div>
          </div>
        )}
      </AnimatePresence>

      {/* WITHDRAW MODAL */}
      <AnimatePresence>
        {showWithdrawModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center p-6 bg-black/60 backdrop-blur-sm">
            <motion.div
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="w-full max-w-md p-6 rounded-2xl border border-border bg-card space-y-6 shadow-2xl relative"
            >
              <div className="space-y-1">
                <h3 className="text-lg font-bold">Withdraw Funds</h3>
                <p className="text-xs text-text-secondary">Withdraw your available balance back to your wallet</p>
              </div>

              <form onSubmit={handleWithdraw} className="space-y-4">
                <div className="space-y-1">
                  <label className="text-[10px] font-bold text-text-secondary uppercase">Destination Address</label>
                  <input
                    type="text"
                    required
                    value={withdrawDest}
                    onChange={(e) => setWithdrawDest(e.target.value)}
                    className="w-full h-10 px-4 rounded-lg border border-border bg-background text-xs focus:outline-none focus:border-primary/50"
                  />
                </div>

                <div className="space-y-1">
                  <label className="text-[10px] font-bold text-text-secondary uppercase">Amount (USDC)</label>
                  <input
                    type="number"
                    step="0.01"
                    min="0.01"
                    required
                    placeholder="0.00"
                    value={withdrawAmount}
                    onChange={(e) => setWithdrawAmount(e.target.value)}
                    className="w-full h-10 px-4 rounded-lg border border-border bg-background text-xs focus:outline-none focus:border-primary/50"
                  />
                </div>

                {withdrawError && (
                  <p className="text-xs text-danger font-medium">{withdrawError}</p>
                )}

                {withdrawTx && (
                  <div className="bg-success/5 border border-success/15 p-3 rounded-lg text-xs space-y-1">
                    <p className="text-success font-semibold flex items-center space-x-1">
                      <CheckCircle size={12} />
                      <span>Withdrawal successful!</span>
                    </p>
                    <p className="text-text-secondary truncate">Tx: {withdrawTx}</p>
                  </div>
                )}

                <div className="flex gap-4 pt-2">
                  <button
                    type="button"
                    onClick={() => { setShowWithdrawModal(false); setWithdrawError(null); setWithdrawTx(null); }}
                    className="flex-1 h-10 border border-border bg-background hover:bg-border/30 rounded-lg text-xs font-semibold transition-all"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    disabled={isWithdrawing}
                    className="flex-1 h-10 bg-primary hover:bg-primary/95 text-white rounded-lg text-xs font-semibold transition-all"
                  >
                    {isWithdrawing ? "Processing..." : "Submit"}
                  </button>
                </div>
              </form>
            </motion.div>
          </div>
        )}
      </AnimatePresence>
    </div>
  );
};
