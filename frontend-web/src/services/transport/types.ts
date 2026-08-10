export interface MarketTransport {
  connect(token: string, onMessage: (msg: any) => void): Promise<void>;
  disconnect(): void;
  subscribe(channels: string[]): void;
  send(action: string, payload: any): void;
}

export type TransportType = 'ws' | 'wt';

// Configurable active transport choice
export const ACTIVE_TRANSPORT: TransportType = 'ws';
