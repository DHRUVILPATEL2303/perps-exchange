import { MarketTransport } from './types';

export class WebTransportTransport implements MarketTransport {
  private wt: any = null;
  private url = 'https://127.0.0.1:4433/wt'; // WebTransport endpoint (HTTP3)
  private writer: any = null;
  private token = '';
  private onMsg: ((msg: any) => void) | null = null;
  private reconnectTimeout: number | null = null;
  private activeSubscriptions: Set<string> = new Set();

  async connect(token: string, onMessage: (msg: any) => void): Promise<void> {
    this.token = token;
    this.onMsg = onMessage;

    if (!('WebTransport' in window)) {
      throw new Error('WebTransport is not supported in this browser');
    }

    try {
      const connectionUrl = `${this.url}?token=${encodeURIComponent(token)}`;
      this.wt = new (window as any).WebTransport(connectionUrl);
      
      await this.wt.ready;
      console.log('[WebTransport] Connection ready');

      // Create stream for sending messages
      const stream = await this.wt.createUnidirectionalStream();
      this.writer = stream.writable.getWriter();

      // Read updates from unidirectional incoming streams
      this.readIncomingStreams();

      if (this.activeSubscriptions.size > 0) {
        this.subscribe(Array.from(this.activeSubscriptions));
      }
    } catch (e) {
      console.error('[WebTransport] Connection failed:', e);
      this.scheduleReconnect();
      throw e;
    }
  }

  private async readIncomingStreams() {
    try {
      const reader = this.wt.incomingUnidirectionalStreams.getReader();
      while (true) {
        const { value: stream, done } = await reader.read();
        if (done) break;
        this.readFromStream(stream);
      }
    } catch (e) {
      console.error('[WebTransport] Read stream closed with error:', e);
    }
  }

  private async readFromStream(stream: any) {
    const reader = stream.readable.getReader();
    const decoder = new TextDecoder();
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        const text = decoder.decode(value);
        if (this.onMsg) {
          try {
            const data = JSON.parse(text);
            this.onMsg(data);
          } catch (e) {
            this.onMsg(text);
          }
        }
      }
    } catch (e) {
      console.error('[WebTransport] Error reading from stream:', e);
    }
  }

  scheduleReconnect() {
    if (this.reconnectTimeout) return;
    this.reconnectTimeout = window.setTimeout(async () => {
      this.reconnectTimeout = null;
      console.log('[WebTransport] Reconnecting...');
      try {
        if (this.token && this.onMsg) {
          await this.connect(this.token, this.onMsg);
        }
      } catch (e) {
        console.error('[WebTransport] Reconnection failed:', e);
      }
    }, 3000);
  }

  disconnect(): void {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }
    if (this.writer) {
      this.writer.releaseLock();
      this.writer = null;
    }
    if (this.wt) {
      this.wt.close();
      this.wt = null;
    }
    this.activeSubscriptions.clear();
    console.log('[WebTransport] Disconnected intentionally');
  }

  subscribe(channels: string[]): void {
    channels.forEach(ch => this.activeSubscriptions.add(ch));
    this.send('subscribe', { channels });
  }

  async send(action: string, payload: any): Promise<void> {
    if (this.writer) {
      const msg = JSON.stringify({ action, ...payload });
      const encoder = new TextEncoder();
      const data = encoder.encode(msg);
      await this.writer.write(data);
    } else {
      console.warn('[WebTransport] Writer not active, cannot send');
    }
  }
}
