use sqlx::{PgPool, Row};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect("postgres://postgres:postgres@localhost:5432/exchange").await?;
    
    let rows_pos = sqlx::query("SELECT user_id::text, symbol, side, size::text, margin::text, liquidation_price::text FROM positions;")
        .fetch_all(&pool)
        .await?;

    println!("--- EXCHANGE POSITIONS ---");
    for row in rows_pos {
        let user_id: String = row.get(0);
        let symbol: String = row.get(1);
        let side: String = row.get(2);
        let size: String = row.get(3);
        let margin: String = row.get(4);
        let liq_price: String = row.get(5);
        println!("User: {} | Symbol: {} | Side: {} | Size: {} | Margin: {} | Liq Price: {}", user_id, symbol, side, size, margin, liq_price);
    }

    let rows_trades = sqlx::query("SELECT id::text, order_id::text, user_id::text, symbol, side, price::text, quantity::text FROM trades;")
        .fetch_all(&pool)
        .await?;

    println!("\n--- PROCESSED TRADES ---");
    for row in rows_trades {
        let id: String = row.get(0);
        let order_id: String = row.get(1);
        let user_id: String = row.get(2);
        let symbol: String = row.get(3);
        let side: String = row.get(4);
        let price: String = row.get(5);
        let quantity: String = row.get(6);
        println!("Trade ID: {} | Order: {} | User: {} | Symbol: {} | Side: {} | Price: {} | Qty: {}", id, order_id, user_id, symbol, side, price, quantity);
    }
    Ok(())
}
