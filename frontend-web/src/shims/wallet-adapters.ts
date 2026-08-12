import { BaseSignerWalletAdapter, EventEmitter } from '@solana/wallet-adapter-base';

const ICON = 'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="%23ab9ff2"%3E%3Cpath d="M12 2L2 22h20L12 2z"/%3E%3C/svg%3E';

class BaseAdapter extends EventEmitter implements BaseSignerWalletAdapter {
  name = 'Wallet';
  url = 'https://phantom.app';
  icon = ICON;
  supportedTransactionVersions: any = null;
  readyState: any = 'Installed';
  publicKey: any = null;
  connecting = false;

  async connect(): Promise<void> {
    this.connecting = true;
    await new Promise((r) => setTimeout(r, 300));
    const pub = crypto.getRandomValues(new Uint8Array(32));
    (this as any).publicKey = { toString: () => Array.from(pub).map(b => b.toString(16).padStart(2, '0')).join('') };
    this.emit('connect', this.publicKey);
    this.connecting = false;
  }

  async disconnect(): Promise<void> {
    (this as any).publicKey = null;
    this.emit('disconnect');
  }

  async signTransaction(tx: any): Promise<any> { return tx; }
  async signAllTransactions(tx: any[]): Promise<any[]> { return tx; }
  async signMessage(message: Uint8Array): Promise<{ signature: Uint8Array }> {
    return { signature: new Uint8Array(64) };
  }
}

export class PhantomWalletAdapter extends BaseAdapter {
  name = 'Phantom';
  url = 'https://phantom.app';
}

export class SolflareWalletAdapter extends BaseAdapter {
  name = 'Solflare';
  url = 'https://solflare.com';
}
