# Walkthrough: Multi-Screen TUI Exchange Client

I have successfully updated the TUI trading client into a multi-screen app resembling a complete web experience.

## Changes Made

1. **State Machine**: Added a `Screen` enum to manage states: `Login`, `Dashboard`, and `Trading`.
2. **Deposit REST Route**: Added `/api/v1/accounts/deposit` to the `api-gateway` router to expose the deposit function for testing and seeding balances.
3. **Login Screen**: Added `crates/tui-client/src/ui.rs` layout for username and password entry. In `main.rs`, this hashes the username to generate a deterministic `user_id` and queries the database. If balance is 0.00, it automatically sends a deposit request of 10,000 USDT to seed the trading profile.
4. **Dashboard Hub**: Displays the user's balance and lists all active perpetual markets fetched from `api-gateway`. It allows selecting a market via arrow keys.
5. **No Code Comments**: Removed all comments from the TUI codebase (`main.rs` and `ui.rs`).

---

## How to Test:

1. **Rebuild and restart the containers** (required due to API Gateway changes):
   ```bash
   docker compose -f docker-compose.all.yaml down
   docker compose -f docker-compose.all.yaml up --build
   ```

2. **Start the TUI Client**:
   ```bash
   cargo run -p tui-client
   ```

3. **Multi-Screen Navigation**:
   * **Login Screen**:
     - Type any username (e.g., `trader_john`) and password.
     - Press `Tab` to cycle between Username and Password fields.
     - Press `Enter` to submit. If this is a new username, it will be automatically registered and seeded with **10,000 USDT**!
   * **Dashboard Screen**:
     - Use `Up` and `Down` arrow keys to highlight a market.
     - Press `Enter` to open the selected market trading desk.
     - Press `Esc` to log out.
   * **Trading Desk**:
     - Use `[` and `]` to resize the layout split.
     - Press `Tab` to focus on the Order Entry form, type values, and press `Enter` to submit order.
     - Press `Esc` to exit trading desk and return to the Dashboard.
