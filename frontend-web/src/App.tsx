import React, { useEffect } from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { useWallet } from '@solana/wallet-adapter-react';
import { LandingPage } from './routes/LandingPage';
import { Dashboard } from './routes/Dashboard';
import { TradingPage } from './routes/TradingPage';
import { Profile } from './routes/Profile';
import { useAuthStore } from './store/authStore';
import { useThemeStore } from './store/themeStore';

import { AuthGuard } from './components/AuthGuard';

// Main App Container Component
export const App: React.FC = () => {
  const { theme } = useThemeStore();
  
  // Set theme class on initial mount
  useEffect(() => {
    const root = window.document.documentElement;
    if (theme === 'dark') {
      root.classList.add('dark');
    } else {
      root.classList.remove('dark');
    }
  }, [theme]);

  return (
    <Routes>
      <Route path="/" element={<LandingPage />} />
      
      {/* Route Stubs (to be fully coded in future pages) protected by AuthGuard */}
      <Route 
        path="/dashboard" 
        element={
          <AuthGuard>
            <Dashboard />
          </AuthGuard>
        } 
      />
      <Route 
        path="/markets/:symbol" 
        element={
          <AuthGuard>
            <TradingPage />
          </AuthGuard>
        } 
      />
      <Route 
        path="/profile" 
        element={
          <AuthGuard>
            <Profile />
          </AuthGuard>
        } 
      />
      
      {/* Fallback route */}
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
};
