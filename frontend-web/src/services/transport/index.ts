import { WebSocketTransport } from './websocket';
import { WebTransportTransport } from './webtransport';
import { MarketTransport, ACTIVE_TRANSPORT } from './types';

export const createTransport = (): MarketTransport => {
  if (ACTIVE_TRANSPORT === 'wt') {
    return new WebTransportTransport();
  }
  return new WebSocketTransport();
};

export * from './types';
