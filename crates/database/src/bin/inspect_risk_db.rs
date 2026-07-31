use sqlx::{PgPool, Row};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect("postgres://postgres:postgres@localhost:5432/perps_risk").await?;
    
    let rows_pos = sqlx::query("SELECT user_id::text, symbol, side, size::text, margin::text, liquidation_price::text FROM positions;")
        .fetch_all(&pool)
        .await?;

    println!("--- RISK ENGINE MIRRORED POSITIONS ---");
    for row in rows_pos {
        let user_id: String = row.get(0);
        let symbol: String = row.get(1);
        let side: String = row.get(2);
        let size: String = row.get(3);
        let margin: String = row.get(4);
        let liq_price: String = row.get(5);
        println!("User: {} | Symbol: {} | Side: {} | Size: {} | Margin: {} | Liq Price: {}", user_id, symbol, side, size, margin, liq_price);
    }
    Ok(())
}
