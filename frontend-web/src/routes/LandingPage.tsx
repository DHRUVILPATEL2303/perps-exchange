import React, { useState, useEffect } from 'react';
import { useWallet } from '@solana/wallet-adapter-react';
import { WalletMultiButton } from '@solana/wallet-adapter-react-ui';
import { useNavigate } from 'react-router-dom';
import { motion } from 'framer-motion';
import { useThemeStore } from '../store/themeStore';
import { useAuthStore } from '../store/authStore';
import {
  TrendingUp,
  ShieldCheck,
  Zap,
  ChevronRight,
  Sun,
  Moon,
  ArrowUpRight,
  Award,
  Globe
} from 'lucide-react';

export const LandingPage: React.FC = () => {
  const { theme, toggleTheme } = useThemeStore();
  const { publicKey, signMessage, disconnect } = useWallet();
  const { token, setAuth, clearAuth } = useAuthStore();
  const navigate = useNavigate();
  
  const [isVerifying, setIsVerifying] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);

  // SIWS (Sign-in-With-Solana) Cryptographic Login Flow
  useEffect(() => {
    const handleLogin = async () => {
      if (!publicKey || !signMessage) return;
      if (token) return; // Already logged in

      setIsVerifying(true);
      setAuthError(null);
      try {
        // 1. Fetch challenge nonce from api-gateway
        const challengeRes = await fetch("http://127.0.0.1:8080/api/v1/auth/challenge", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ public_key: publicKey.toBase58() }),
        });
        if (!challengeRes.ok) throw new Error("Authentication challenge failed");
        const { nonce } = await challengeRes.json();

        // 2. Sign SIWS message
        const message = `Sign-in to Perpetuals Exchange: ${nonce}`;
        const messageBytes = new TextEncoder().encode(message);
        const signatureBytes = await signMessage(messageBytes);
        
        // Helper to convert signature to base58 representation
        const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
        const encodeBase58 = (buffer: Uint8Array): string => {
          const digits = [0];
          for (let i = 0; i < buffer.length; i++) {
            let val = buffer[i];
            for (let j = 0; j < digits.length; j++) {
              val += digits[j] << 8;
              digits[j] = val % 58;
              val = Math.floor(val / 58);
            }
            while (val > 0) {
              digits.push(val % 58);
              val = Math.floor(val / 58);
            }
          }
          let string = '';
          for (let i = 0; i < buffer.length && buffer[i] === 0; i++) {
            string += '1';
          }
          for (let i = digits.length - 1; i >= 0; i--) {
            string += BASE58_ALPHABET[digits[i]];
          }
          return string;
        };
        const signatureBase58 = encodeBase58(signatureBytes);

        // 3. Submit credentials to login
        const loginRes = await fetch("http://127.0.0.1:8080/api/v1/auth/login", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            public_key: publicKey.toBase58(),
            signature: signatureBase58,
            nonce: nonce,
          }),
        });
        if (!loginRes.ok) throw new Error("Invalid cryptographic signature");
        const { token: jwtToken, user_id: userId } = await loginRes.json();

        setAuth(jwtToken, userId, publicKey.toBase58());
      } catch (err: any) {
        console.error(err);
        setAuthError(err.message || "Failed to authenticate wallet");
        disconnect();
        clearAuth();
      } finally {
        setIsVerifying(false);
      }
    };

    handleLogin();
  }, [publicKey, signMessage, token, setAuth, clearAuth, disconnect]);

  // Handle wallet disconnect cleanup
  useEffect(() => {
    if (!publicKey) {
      clearAuth();
    }
  }, [publicKey, clearAuth]);

  return (
    <div className="min-h-screen bg-background text-text transition-colors duration-200">
      {/* Navigation Header */}
      <nav className="fixed top-0 left-0 right-0 z-50 flex items-center justify-between px-6 md:px-12 py-4 border-b border-border bg-background/80 backdrop-blur-md">
        <div className="flex items-center space-x-8">
          {/* Logo SVG */}
          <div className="flex items-center space-x-3 cursor-pointer">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M12 2L2 22h20L12 2zm0 4l6.5 13h-13L12 6z" fill="var(--primary)" />
            </svg>
            <span className="font-sans font-extrabold text-xl tracking-tight">dpkv perps</span>
          </div>

          <div className="hidden md:flex items-center space-x-6 text-sm font-medium text-text-secondary">
            <a href="#features" className="hover:text-text transition-colors">Features</a>
            <a href="#stats" className="hover:text-text transition-colors">Stats</a>
            <a href="#why-choose-us" className="hover:text-text transition-colors">Security</a>
          </div>
        </div>

        <div className="flex items-center space-x-4">
          <button
            onClick={toggleTheme}
            className="p-2 rounded-lg border border-border bg-card hover:bg-border/30 transition-all"
            aria-label="Toggle theme"
          >
            {theme === 'dark' ? <Sun size={18} className="text-amber-400" /> : <Moon size={18} className="text-indigo-600" />}
          </button>

          {!token ? (
            <WalletMultiButton className="!bg-primary hover:!bg-primary/95 !h-10 !px-4 !rounded-lg !text-sm !font-semibold !transition-all !border-none" />
          ) : (
            <button
              onClick={() => navigate('/dashboard')}
              className="flex items-center space-x-2 px-4 py-2 bg-primary hover:bg-primary/90 text-white rounded-lg text-sm font-semibold transition-all shadow-lg shadow-primary/20"
            >
              <span>Go to Dashboard</span>
              <ChevronRight size={16} />
            </button>
          )}
        </div>
      </nav>

      {/* Hero Section */}
      <section className="relative flex flex-col items-center justify-center min-h-screen px-6 text-center pt-20 overflow-hidden">
        {/* Background Decorative Gradients */}
        <div className="absolute top-1/4 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[500px] h-[500px] bg-primary/10 rounded-full blur-[120px] pointer-events-none" />
        
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, ease: 'easeOut' }}
          className="max-w-4xl mx-auto space-y-8"
        >
          <div className="inline-flex items-center space-x-2 px-3 py-1 rounded-full border border-primary/20 bg-primary/5 text-primary text-xs font-semibold">
            <span>Solana Devnet Perps Exchange is Live</span>
            <ArrowUpRight size={14} />
          </div>

          <h1 className="font-sans font-extrabold text-5xl md:text-8xl tracking-tight leading-none text-text">
            Modern <span className="bg-gradient-to-r from-primary to-accent bg-clip-text text-transparent">finance.</span>
          </h1>

          <p className="max-w-2xl mx-auto text-lg md:text-xl text-text-secondary font-medium leading-relaxed">
            Your brokerage, your exchange, your money — in the same place. Trade perpetual futures directly from your self-custody Solana wallet.
          </p>

          <div className="flex flex-col sm:flex-row items-center justify-center gap-4 pt-4">
            {!token ? (
              <div className="relative group">
                <WalletMultiButton className="!bg-primary hover:!bg-primary/95 !h-12 !px-6 !rounded-lg !text-sm !font-semibold !transition-all !shadow-lg shadow-primary/20" />
                {isVerifying && (
                  <div className="absolute inset-0 bg-background/80 flex items-center justify-center rounded-lg">
                    <span className="text-xs font-semibold text-primary animate-pulse">Verifying Signature...</span>
                  </div>
                )}
              </div>
            ) : (
              <button
                onClick={() => navigate('/dashboard')}
                className="flex items-center space-x-2 px-6 py-3 bg-primary hover:bg-primary/90 text-white rounded-lg text-sm font-semibold transition-all shadow-lg shadow-primary/20"
              >
                <span>Launch App</span>
                <ChevronRight size={18} />
              </button>
            )}
            <a
              href="#features"
              className="flex items-center justify-center w-full sm:w-auto h-12 px-6 rounded-lg border border-border bg-card hover:bg-border/30 text-sm font-semibold transition-all"
            >
              Explore Perps
            </a>
          </div>

          {authError && (
            <p className="text-xs font-medium text-danger bg-danger/5 border border-danger/10 px-3 py-2 rounded-lg inline-block">
              Authentication Error: {authError}
            </p>
          )}
        </motion.div>
      </section>

      {/* Exchange Stats Section */}
      <section id="stats" className="py-24 border-t border-border bg-card/20">
        <div className="max-w-7xl mx-auto px-6 md:px-12">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
            <motion.div
              whileHover={{ y: -5 }}
              className="p-8 rounded-2xl border border-border bg-card text-center space-y-2"
            >
              <span className="text-text-secondary text-sm font-semibold tracking-wider uppercase">24H Trading Volume</span>
              <p className="text-3xl md:text-4xl font-extrabold text-primary font-mono">$1,245,671,283</p>
            </motion.div>
            <motion.div
              whileHover={{ y: -5 }}
              className="p-8 rounded-2xl border border-border bg-card text-center space-y-2"
            >
              <span className="text-text-secondary text-sm font-semibold tracking-wider uppercase">Total Open Interest</span>
              <p className="text-3xl md:text-4xl font-extrabold text-accent font-mono">$184,291,048</p>
            </motion.div>
            <motion.div
              whileHover={{ y: -5 }}
              className="p-8 rounded-2xl border border-border bg-card text-center space-y-2"
            >
              <span className="text-text-secondary text-sm font-semibold tracking-wider uppercase">Registered Accounts</span>
              <p className="text-3xl md:text-4xl font-extrabold text-success font-mono">48,291</p>
            </motion.div>
          </div>
        </div>
      </section>

      {/* Features Grid */}
      <section id="features" className="py-24 border-t border-border">
        <div className="max-w-7xl mx-auto px-6 md:px-12 space-y-16">
          <div className="text-center space-y-4">
            <h2 className="text-3xl md:text-5xl font-extrabold tracking-tight">Built for elite traders.</h2>
            <p className="text-text-secondary max-w-xl mx-auto text-sm md:text-base">
              Experience spot-like execution speed combined with full self-custody position monitoring.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
            <div className="p-8 rounded-2xl border border-border bg-card space-y-6 hover:border-primary/50 transition-all duration-300">
              <div className="p-3 w-fit bg-primary/10 rounded-xl text-primary">
                <Zap size={24} />
              </div>
              <div className="space-y-2">
                <h3 className="text-lg font-bold">Sub-millisecond Matching</h3>
                <p className="text-text-secondary text-sm leading-relaxed">
                  FIFO Order matching handled entirely off-chain in memory, backed by our Rust matching engine for instant fills.
                </p>
              </div>
            </div>

            <div className="p-8 rounded-2xl border border-border bg-card space-y-6 hover:border-primary/50 transition-all duration-300">
              <div className="p-3 w-fit bg-accent/10 rounded-xl text-accent">
                <TrendingUp size={24} />
              </div>
              <div className="space-y-2">
                <h3 className="text-lg font-bold">Dynamic Margin Modes</h3>
                <p className="text-text-secondary text-sm leading-relaxed">
                  Configure position margins dynamically using cross or isolated modes. Keep risk separated.
                </p>
              </div>
            </div>

            <div className="p-8 rounded-2xl border border-border bg-card space-y-6 hover:border-primary/50 transition-all duration-300">
              <div className="p-3 w-fit bg-success/10 rounded-xl text-success">
                <ShieldCheck size={24} />
              </div>
              <div className="space-y-2">
                <h3 className="text-lg font-bold">Isolated custody</h3>
                <p className="text-text-secondary text-sm leading-relaxed">
                  Deposit directly to program ATAs. Secure multi-sig custodian wallet holds assets safely outside of the execution state.
                </p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Why Choose Us */}
      <section id="why-choose-us" className="py-24 border-t border-border bg-card/10">
        <div className="max-w-7xl mx-auto px-6 md:px-12 space-y-16">
          <div className="text-center space-y-4">
            <h2 className="text-3xl md:text-5xl font-extrabold tracking-tight">Uncompromising performance.</h2>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-12">
            <div className="flex items-start space-x-6">
              <div className="p-3 bg-primary/10 text-primary rounded-lg mt-1">
                <Award size={20} />
              </div>
              <div className="space-y-2">
                <h4 className="text-base font-bold">Deep Liquidity Feeds</h4>
                <p className="text-text-secondary text-xs md:text-sm">
                  Leveraging high-frequency price aggregates matching real-time market depths for tight spread order placement.
                </p>
              </div>
            </div>

            <div className="flex items-start space-x-6">
              <div className="p-3 bg-primary/10 text-primary rounded-lg mt-1">
                <Globe size={20} />
              </div>
              <div className="space-y-2">
                <h4 className="text-base font-bold">Continuous Funding Settlements</h4>
                <p className="text-text-secondary text-xs md:text-sm">
                  Mark-to-market settlements executed hourly ensuring pricing stays tightly bound to the index spot price.
                </p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-border py-12 bg-background">
        <div className="max-w-7xl mx-auto px-6 md:px-12 flex flex-col md:flex-row items-center justify-between gap-6">
          <div className="flex items-center space-x-3">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M12 2L2 22h20L12 2zm0 4l6.5 13h-13L12 6z" fill="var(--primary)" />
            </svg>
            <span className="font-sans font-extrabold tracking-tight">dpkv perps</span>
          </div>

          <p className="text-text-secondary text-xs">
            © {new Date().getFullYear()} dpkv perps. All rights reserved. Self-custodial perpetual futures trading.
          </p>
        </div>
      </footer>
    </div>
  );
};
