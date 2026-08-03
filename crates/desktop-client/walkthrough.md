# Walkthrough: Native Desktop Client (Tauri v2 Migration)

I have successfully updated the Tauri configuration and crate dependencies to support the **Tauri v2** CLI you have installed.

## Changes Made

1. **Cargo Configuration**: Updated `crates/desktop-client/src-tauri/Cargo.toml` to use Tauri v2 dependencies (`tauri = "2.1"` and `tauri-build = "2.0"`).
2. **Schema Upgrade**: Rewrote `tauri.conf.json` using the Tauri v2 schema structure:
   - Root-level `"identifier"` and `"productName"`.
   - `"build"` using `"frontendDist"`.
   - `"app"` with `"windows"` nested configuration including window `"label": "main"`.
3. **Capabilities System**: Added a default capability declaration at `crates/desktop-client/src-tauri/capabilities/default.json` to grant the main window execution permissions.
4. **Web Dashboard Frontend**: Kept the lightweight, comment-free HTML/JS/CSS client at `crates/desktop-client/ui/`.
5. **CORS Middleware integration**: Integrated permissive `actix-cors` middleware into the API Gateway [bootstrap.rs](file:///Users/dhruvilpatel/Developer/perps-exchange/services/api-gateway/src/bootstrap.rs) to permit the Tauri webview to successfully dispatch REST and WebSocket frames to `http://localhost:8080` without origin blocks.
6. **Dockerfile Compilation Isolation**: Modified the root [Dockerfile](file:///Users/dhruvilpatel/Developer/perps-exchange/Dockerfile) to build *only* the 9 backend service crates (`-p market-service`, etc.), ignoring client crates (`desktop-client`, `tui-client`) to prevent dependency compiles requiring GUI/GTK development headers inside the slim Docker Linux container.
7. **No Comments**: Maintained a clean codebase without comments.

---

## How to Compile & Run:

1. **Rebuild the Docker stack** (required to compile the API Gateway with CORS enabled and bypass GUI library compile requirements):
   ```bash
   docker compose -f docker-compose.all.yaml down
   docker compose -f docker-compose.all.yaml up --build
   ```

2. **Seed the Markets** (Once the containers are up):
   ```bash
   python3 /Users/dhruvilpatel/.gemini/antigravity-ide/brain/d85192c3-2b49-408c-9358-591eea49d4a8/scratch/seed_markets.py
   ```

3. **Launch the Desktop Client**:
   ```bash
   cargo tauri dev
   ```

---

## Navigation & Controls:
* **Authentication**: Input any username/password. If the user doesn't exist, it auto-creates the account and seeds **10,000 USDT**.
* **Market Directory**: Single-click to select a market row, and click **Enter Selected Market** (or double-click) to open the Trading Desk.
* **Trading Desk**: Submits limit/market orders, updates the depth depth bar chart instantly, lists executed trades, and plots price movements. Click **Back** in the top left to return to the Directory.
