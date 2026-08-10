import React from 'react';
import ReactDOM from 'react-dom/client';
import { App } from './App';
import { AllProviders } from './providers/AllProviders';

// Import CSS styles
import './index.css';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <AllProviders>
      <App />
    </AllProviders>
  </React.StrictMode>
);
