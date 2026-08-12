import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  root: path.resolve(__dirname, 'frontend-web'),
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'frontend-web/src'),
      '@solana/wallet-adapter-wallets': path.resolve(__dirname, 'frontend-web/src/shims/wallet-adapters.ts'),
    },
  },
  server: {
    port: 3000,
  },
})
