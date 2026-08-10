import React, { useEffect } from 'react';
import { Navigate } from 'react-router-dom';
import { useWallet } from '@solana/wallet-adapter-react';
import { useAuthStore } from '../store/authStore';

interface AuthGuardProps {
  children: React.ReactNode;
}

export const AuthGuard: React.FC<AuthGuardProps> = ({ children }) => {
  const { token, clearAuth } = useAuthStore();
  const { publicKey } = useWallet();

  // If the wallet disconnects, clear our cached token immediately
  useEffect(() => {
    if (!publicKey) {
      clearAuth();
    }
  }, [publicKey, clearAuth]);

  // If we have no active JWT token or wallet, redirect to Landing Page
  if (!token || !publicKey) {
    return <Navigate to="/" replace />;
  }

  return <>{children}</>;
};
