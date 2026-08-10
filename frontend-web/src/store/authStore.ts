import { create } from 'zustand';

interface AuthState {
  token: string | null;
  userId: string | null;
  pubkey: string | null;
  setAuth: (token: string, userId: string, pubkey: string) => void;
  clearAuth: () => void;
}

export const useAuthStore = create<AuthState>((set) => ({
  token: localStorage.getItem('auth_token'),
  userId: localStorage.getItem('auth_user_id'),
  pubkey: localStorage.getItem('auth_pubkey'),
  setAuth: (token, userId, pubkey) => {
    localStorage.setItem('auth_token', token);
    localStorage.setItem('auth_user_id', userId);
    localStorage.setItem('auth_pubkey', pubkey);
    set({ token, userId, pubkey });
  },
  clearAuth: () => {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('auth_user_id');
    localStorage.removeItem('auth_pubkey');
    set({ token: null, userId: null, pubkey: null });
  },
}));
