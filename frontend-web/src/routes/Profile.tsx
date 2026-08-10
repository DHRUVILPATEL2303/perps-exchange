import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useWallet } from '@solana/wallet-adapter-react';
import { motion, AnimatePresence } from 'framer-motion';
import { useThemeStore } from '../store/themeStore';
import { useAuthStore } from '../store/authStore';
import {
  User,
  ArrowLeft,
  Sun,
  Moon,
  Copy,
  CheckCircle,
  MessageSquare,
  Key,
  ShieldAlert,
  LogOut,
  ChevronRight,
  ExternalLink,
  Info
} from 'lucide-react';

export const Profile: React.FC = () => {
  const navigate = useNavigate();
  const { theme, toggleTheme } = useThemeStore();
  const { token, userId, pubkey, clearAuth } = useAuthStore();
  const { disconnect } = useWallet();

  const [copiedUid, setCopiedUid] = useState(false);
  const [copiedPub, setCopiedPub] = useState(false);

  // Telegram link states
  const [telegramToken, setTelegramToken] = useState<string | null>(null);
  const [isGeneratingTgToken, setIsGeneratingTgToken] = useState(false);
  const [tgError, setTgError] = useState<string | null>(null);
  const [copiedTgToken, setCopiedTgToken] = useState(false);

  const handleCopyUid = () => {
    if (!userId) return;
    navigator.clipboard.writeText(userId);
    setCopiedUid(true);
    setTimeout(() => setCopiedUid(false), 2000);
  };

  const handleCopyPub = () => {
    if (!pubkey) return;
    navigator.clipboard.writeText(pubkey);
    setCopiedPub(true);
    setTimeout(() => setCopiedPub(false), 2000);
  };

  const handleGenerateTelegramToken = async () => {
    if (!token) return;
    setIsGeneratingTgToken(true);
    setTgError(null);
    try {
      const res = await fetch("http://127.0.0.1:8080/api/v1/auth/telegram-token", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`
        }
      });
      if (!res.ok) {
        throw new Error("Failed to generate token");
      }
      const data = await res.json();
      setTelegramToken(data.token);
    } catch (e: any) {
      setTgError(e.message || "Failed to generate linkage token");
    } finally {
      setIsGeneratingTgToken(false);
    }
  };

  const handleCopyTgToken = () => {
    if (!telegramToken) return;
    navigator.clipboard.writeText(telegramToken);
    setCopiedTgToken(true);
    setTimeout(() => setCopiedTgToken(false), 2000);
  };

  const handleLogout = () => {
    disconnect();
    clearAuth();
    navigate('/');
  };

  return (
    <div className="min-h-screen bg-background text-text transition-colors duration-200">
      
      {/* Navigation Header */}
      <nav className="fixed top-0 left-0 right-0 z-40 flex items-center justify-between px-6 h-14 border-b border-border bg-background/80 backdrop-blur-md">
        <div className="flex items-center space-x-6">
          <button
            onClick={() => navigate('/dashboard')}
            className="flex items-center space-x-1.5 text-xs text-text-secondary hover:text-text transition-all"
          >
            <ArrowLeft size={14} />
            <span>Dashboard</span>
          </button>
          
          <div className="h-4 w-px bg-border" />
          
          <span className="font-sans font-bold text-sm">Account Settings</span>
        </div>

        <div className="flex items-center space-x-3">
          <button
            onClick={toggleTheme}
            className="p-1.5 rounded-lg border border-border bg-card hover:bg-border/30 transition-all"
            aria-label="Toggle theme"
          >
            {theme === 'dark' ? <Sun size={14} className="text-amber-400" /> : <Moon size={14} className="text-indigo-600" />}
          </button>
        </div>
      </nav>

      {/* Main Profile Grid Layout */}
      <main className="pt-24 pb-12 px-6 max-w-4xl mx-auto space-y-8">
        
        {/* Profile header banner */}
        <div className="flex items-center space-x-4 border-b border-border/40 pb-6">
          <div className="p-4 bg-primary/10 text-primary rounded-2xl">
            <User size={32} />
          </div>
          <div className="space-y-1">
            <h2 className="text-2xl font-extrabold tracking-tight">Profile</h2>
            <p className="text-xs text-text-secondary">Manage your self-custody wallet connection and notification settings</p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          
          {/* Left Panel: Account details */}
          <section className="space-y-6">
            <div className="p-6 rounded-2xl border border-border bg-card space-y-6">
              <h3 className="text-sm font-bold text-text-secondary uppercase tracking-wider">Account Credentials</h3>

              {/* User UUID field */}
              <div className="space-y-1">
                <span className="text-[10px] text-text-secondary font-semibold uppercase">Platform User ID</span>
                <div className="flex items-center justify-between border border-border/50 bg-background/50 rounded-xl px-4 py-2.5 font-mono text-xs">
                  <span className="truncate max-w-[85%]">{userId}</span>
                  <button
                    onClick={handleCopyUid}
                    className="p-1.5 rounded-lg border border-border/50 hover:bg-border/30 text-text-secondary hover:text-text transition-all"
                  >
                    {copiedUid ? <CheckCircle size={12} className="text-success" /> : <Copy size={12} />}
                  </button>
                </div>
              </div>

              {/* Public key field */}
              <div className="space-y-1">
                <span className="text-[10px] text-text-secondary font-semibold uppercase">Solana Public Key</span>
                <div className="flex items-center justify-between border border-border/50 bg-background/50 rounded-xl px-4 py-2.5 font-mono text-xs">
                  <span className="truncate max-w-[85%]">{pubkey}</span>
                  <button
                    onClick={handleCopyPub}
                    className="p-1.5 rounded-lg border border-border/50 hover:bg-border/30 text-text-secondary hover:text-text transition-all"
                  >
                    {copiedPub ? <CheckCircle size={12} className="text-success" /> : <Copy size={12} />}
                  </button>
                </div>
              </div>

              {/* Logout */}
              <button
                onClick={handleLogout}
                className="w-full flex items-center justify-center space-x-2 h-10 border border-danger/25 bg-danger/5 hover:bg-danger/10 text-danger rounded-lg text-xs font-bold transition-all"
              >
                <LogOut size={14} />
                <span>Disconnect Wallet</span>
              </button>
            </div>

            {/* Fee Tiers placeholder */}
            <div className="p-6 rounded-2xl border border-border bg-card space-y-3">
              <h3 className="text-sm font-bold text-text-secondary uppercase tracking-wider">Trading Fee Tiers</h3>
              <div className="flex justify-between items-center bg-background/50 p-4 rounded-xl border border-border/40">
                <div className="space-y-0.5">
                  <span className="text-xs font-bold">Standard Tier 1</span>
                  <p className="text-[10px] text-text-secondary">Maker: 0.00% / Taker: 0.05%</p>
                </div>
                <span className="text-[10px] bg-primary/20 text-primary px-2 py-0.5 rounded font-bold uppercase">Active</span>
              </div>
            </div>
          </section>

          {/* Right Panel: Telegram Bot Integration */}
          <section className="space-y-6">
            <div className="p-6 rounded-2xl border border-border bg-card space-y-6">
              <div className="flex items-start space-x-3">
                <div className="p-2 bg-primary/10 text-primary rounded-xl mt-0.5">
                  <MessageSquare size={18} />
                </div>
                <div className="space-y-1">
                  <h3 className="text-sm font-bold">Telegram Notification Bot</h3>
                  <p className="text-[11px] text-text-secondary leading-relaxed">
                    Receive instant push notifications for order fills, position funding settlements, and liquidation alerts directly on Telegram.
                  </p>
                </div>
              </div>

              {!telegramToken ? (
                <button
                  onClick={handleGenerateTelegramToken}
                  disabled={isGeneratingTgToken}
                  className="w-full h-11 bg-primary hover:bg-primary/95 text-white rounded-lg text-xs font-bold transition-all shadow-md shadow-primary/20"
                >
                  {isGeneratingTgToken ? "Generating token..." : "Link Telegram Bot"}
                </button>
              ) : (
                <motion.div
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  className="space-y-4 border-t border-border/40 pt-4"
                >
                  <div className="space-y-1">
                    <span className="text-[9px] font-bold text-text-secondary uppercase">Your Linking Token</span>
                    <div className="flex items-center justify-between border border-border/60 bg-background/50 rounded-xl px-4 py-2.5 font-mono text-xs">
                      <span className="truncate">{telegramToken}</span>
                      <button
                        onClick={handleCopyTgToken}
                        className="p-1 rounded hover:bg-border/30 text-text-secondary hover:text-text transition-all"
                      >
                        {copiedTgToken ? <CheckCircle size={12} className="text-success" /> : <Copy size={12} />}
                      </button>
                    </div>
                  </div>

                  <a
                    href={`https://t.me/dpkv_perps_bot?start=${telegramToken}`}
                    target="_blank"
                    rel="noreferrer"
                    className="w-full flex items-center justify-center space-x-2 h-11 bg-success hover:bg-success/95 text-white rounded-lg text-xs font-bold transition-all shadow-md shadow-success/15"
                  >
                    <span>Open Telegram & Start Bot</span>
                    <ExternalLink size={12} />
                  </a>

                  <p className="text-[9px] text-text-secondary leading-relaxed flex items-start space-x-1">
                    <Info size={12} className="flex-shrink-0 mt-0.5" />
                    <span>Deep-link will automatically input the token. If it doesn't, send `/start {telegramToken}` to the bot. Token expires in 5 minutes.</span>
                  </p>
                </motion.div>
              )}

              {tgError && (
                <p className="text-xs font-medium text-danger">{tgError}</p>
              )}
            </div>

            {/* API Keys placeholder */}
            <div className="p-6 rounded-2xl border border-border bg-card space-y-4">
              <div className="flex items-start space-x-3">
                <div className="p-2 bg-primary/10 text-primary rounded-xl mt-0.5">
                  <Key size={18} />
                </div>
                <div className="space-y-1">
                  <h3 className="text-sm font-bold">API Access Keys</h3>
                  <p className="text-[11px] text-text-secondary leading-relaxed">
                    Create API keys to connect trading bots or algorithm models directly to the exchange matching pipeline.
                  </p>
                </div>
              </div>

              <div className="bg-background/40 border border-border/50 p-4 rounded-xl flex items-center space-x-3">
                <ShieldAlert size={18} className="text-text-secondary" />
                <span className="text-[10px] text-text-secondary font-semibold">
                  API Key creation and webhook subscriptions will be enabled in a future updates package.
                </span>
              </div>
            </div>
          </section>
        </div>
      </main>
    </div>
  );
};
