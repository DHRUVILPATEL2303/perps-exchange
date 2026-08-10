import { MarketTransport } from './types';

export class WebSocketTransport implements MarketTransport {
  private ws: WebSocket | null = null;
  private url = 'ws://127.0.0.1:8080/ws';
  private reconnectTimeout: number | null = null;
  private isConnecting = false;
  private token = '';
  private onMsg: ((msg: any) => void) | null = null;
  private activeSubscriptions: Set<string> = new Set();

  async connect(token: string, onMessage: (msg: any) => void): Promise<void> {
    this.token = token;
    this.onMsg = onMessage;
    this.isConnecting = true;

    return new Promise((resolve, reject) => {
      try {
        const connectionUrl = `${this.url}?token=${encodeURIComponent(token)}`;
        this.ws = new WebSocket(connectionUrl);

        this.ws.onopen = () => {
          console.log('[WebSocket] Connection opened successfully');
          this.isConnecting = false;
          // Re-subscribe to any active channels
          if (this.activeSubscriptions.size > 0) {
            this.subscribe(Array.from(this.activeSubscriptions));
          }
          resolve();
        };

        this.ws.onmessage = (event) => {
          if (this.onMsg) {
            try {
              const data = JSON.parse(event.data);
              this.onMsg(data);
            } catch (e) {
              // Handle raw strings if necessary
              this.onMsg(event.data);
            }
          }
        };

        this.ws.onerror = (err) => {
          console.error('[WebSocket] Error occurred:', err);
          if (this.isConnecting) {
            this.isConnecting = false;
            reject(err);
          }
        };

        this.ws.onclose = () => {
          console.log('[WebSocket] Connection closed');
          this.ws = null;
          this.scheduleReconnect();
        };
      } catch (e) {
        this.isConnecting = false;
        reject(e);
      }
    });
  }

  scheduleReconnect() {
    if (this.reconnectTimeout) return;
    this.reconnectTimeout = window.setTimeout(async () => {
      this.reconnectTimeout = null;
      console.log('[WebSocket] Attempting automatic reconnection...');
      try {
        if (this.token && this.onMsg) {
          await this.connect(this.token, this.onMsg);
        }
      } catch (e) {
        console.error('[WebSocket] Reconnection failed:', e);
      }
    }, 3000);
  }

  disconnect(): void {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }
    if (this.ws) {
      this.ws.onclose = null; // Prevent reconnect loop on intentional disconnect
      this.ws.close();
      this.ws = null;
    }
    this.activeSubscriptions.clear();
    console.log('[WebSocket] Disconnected intentionally');
  }

  subscribe(channels: string[]): void {
    channels.forEach(ch => this.activeSubscriptions.add(ch));
    this.send('subscribe', { channels });
  }

  send(action: string, payload: any): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      const msg = JSON.stringify({ action, ...payload });
      this.ws.send(msg);
    } else {
      console.warn('[WebSocket] Warning: Cannot send message, socket not open');
    }
  }
}
