use std::sync::Arc;
use anyhow::Result;
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use rdkafka::message::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use uuid::Uuid;
use crate::infrastructure::repositories::postgres_position_repository::PositionRepository;
use crate::price_tracker::price_tracker::PriceTracker;

#[derive(Deserialize, Debug)]
pub struct TradeEvent {
    pub id: Uuid,
    pub symbol: String,
    pub maker_order_id: Uuid,
    pub taker_order_id: Uuid,
    pub maker_user_id: Uuid,
    pub taker_user_id: Uuid,
    pub price: Decimal,
    pub quantity: Decimal,
    pub taker_side: String,
}

pub struct TradeConsumer {
    consumer: StreamConsumer,
    repository: Arc<PositionRepository>,
    price_tracker: PriceTracker,
}


impl TradeConsumer {
    pub fn new(
        brokers: &str,
        group_id: &str,
        repository: Arc<PositionRepository>,
        price_tracker: PriceTracker,
    ) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .create()?;
        consumer.subscribe(&["execution-reports"])?;
        Ok(Self {
            consumer,
            repository,
            price_tracker,
        })
    }


    pub async fn run(self) {
        let mut stream = self.consumer.stream();

        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Err(e) => {
                    tracing::error!("Kafka trade consumer error: {}", e);
                }
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        match serde_json::from_slice::<TradeEvent>(payload) {
                            Ok(event) => {
                                self.price_tracker.set_perp_price(event.price);
                                if let Err(e) = self.process_trade(event).await {
                                    tracing::error!("Failed to mirror position: {:?}", e);
                                }
                            }

                            Err(e) => {
                                tracing::error!(
                                    "Failed to deserialize TradeEvent in Risk Engine: {:?}, payload: {:?}",
                                    e,
                                    String::from_utf8_lossy(payload)
                                );
                            }
                        }
                        let _ = self.consumer.commit_message(&msg, CommitMode::Async);
                    }
                }
            }
        }
    }

    async fn process_trade(&self, event: TradeEvent) -> Result<()> {
        if event.taker_side == "CANCEL" {
            return Ok(());
        }

        let maker_side = if event.taker_side == "BUY" { "SHORT" } else { "LONG" };
        let taker_side = if event.taker_side == "BUY" { "LONG" } else { "SHORT" };

        self.update_mirrored_position(
            event.taker_user_id,
            &event.symbol,
            taker_side,
            event.price,
            event.quantity,
        ).await?;

        self.update_mirrored_position(
            event.maker_user_id,
            &event.symbol,
            maker_side,
            event.price,
            event.quantity,
        ).await?;

        Ok(())
    }

    async fn update_mirrored_position(
        &self,
        user_id: Uuid,
        symbol: &str,
        trade_side: &str,
        trade_price: Decimal,
        trade_qty: Decimal,
    ) -> Result<()> {
        let opposite_side = if trade_side == "LONG" { "SHORT" } else { "LONG" };
        let leverage = 20;
        let mmr = Decimal::new(5, 3);

        let opposite_pos = self.repository.find_by_user_symbol_side(user_id, symbol, opposite_side).await?;

        if let Some((opp_size, opp_entry, opp_margin, opp_lev)) = opposite_pos {
            if opp_size > Decimal::ZERO {
                if opp_size > trade_qty {
                    let released_margin = (trade_qty / opp_size) * opp_margin;
                    let new_size = opp_size - trade_qty;
                    let new_margin = opp_margin - released_margin;
                    let new_liq = if opposite_side == "LONG" {
                        opp_entry - (new_margin / new_size) / (Decimal::ONE - mmr)
                    } else {
                        opp_entry + (new_margin / new_size) / (Decimal::ONE + mmr)
                    };

                    self.repository.update_position(
                        user_id,
                        symbol,
                        opposite_side,
                        new_size,
                        opp_entry,
                        new_margin,
                        opp_lev,
                        new_liq,
                    ).await?;
                    return Ok(());
                } else {
                    self.repository.update_position(
                        user_id,
                        symbol,
                        opposite_side,
                        Decimal::ZERO,
                        Decimal::ZERO,
                        Decimal::ZERO,
                        opp_lev,
                        Decimal::ZERO,
                    ).await?;

                    let remaining_qty = trade_qty - opp_size;
                    if remaining_qty > Decimal::ZERO {
                        let new_margin = (remaining_qty * trade_price) / Decimal::from(leverage);
                        let new_liq = if trade_side == "LONG" {
                            trade_price - (new_margin / remaining_qty) / (Decimal::ONE - mmr)
                        } else {
                            trade_price + (new_margin / remaining_qty) / (Decimal::ONE + mmr)
                        };

                        self.repository.update_position(
                            user_id,
                            symbol,
                            trade_side,
                            remaining_qty,
                            trade_price,
                            new_margin,
                            leverage,
                            new_liq,
                        ).await?;
                    }
                    return Ok(());
                }
            }
        }

        let existing_pos = self.repository.find_by_user_symbol_side(user_id, symbol, trade_side).await?;

        if let Some((ext_size, ext_entry, ext_margin, ext_lev)) = existing_pos {
            if ext_size > Decimal::ZERO {
                let new_size = ext_size + trade_qty;
                let new_entry = ((ext_size * ext_entry) + (trade_qty * trade_price)) / new_size;
                let added_margin = (trade_qty * trade_price) / Decimal::from(ext_lev);
                let new_margin = ext_margin + added_margin;
                let new_liq = if trade_side == "LONG" {
                    new_entry - (new_margin / new_size) / (Decimal::ONE - mmr)
                } else {
                    new_entry + (new_margin / new_size) / (Decimal::ONE + mmr)
                };

                self.repository.update_position(
                    user_id,
                    symbol,
                    trade_side,
                    new_size,
                    new_entry,
                    new_margin,
                    ext_lev,
                    new_liq,
                ).await?;
                return Ok(());
            }
        }

        let new_margin = (trade_qty * trade_price) / Decimal::from(leverage);
        let new_liq = if trade_side == "LONG" {
            trade_price - (new_margin / trade_qty) / (Decimal::ONE - mmr)
        } else {
            trade_price + (new_margin / trade_qty) / (Decimal::ONE + mmr)
        };

        self.repository.update_position(
            user_id,
            symbol,
            trade_side,
            trade_qty,
            trade_price,
            new_margin,
            leverage,
            new_liq,
        ).await?;

        Ok(())
    }
}
