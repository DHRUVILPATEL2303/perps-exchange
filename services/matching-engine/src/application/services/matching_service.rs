use std::collections::{BTreeMap, VecDeque};
use chrono::Utc;
use rust_decimal::Decimal;
use uuid::Uuid;
use crate::domain::entities::order::{BookOrder, OrderSide, OrderStatus, OrderType};
use crate::domain::entities::trade::Trade;

pub struct OrderBook {
    pub symbol: String,
    pub bids: BTreeMap<Decimal, VecDeque<BookOrder>>,
    pub asks: BTreeMap<Decimal, VecDeque<BookOrder>>,
}

impl OrderBook {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub fn add_order(&mut self, order: BookOrder) {
        let book_side = match order.side {
            OrderSide::Buy => &mut self.bids,
            OrderSide::Sell => &mut self.asks,
        };
        book_side
            .entry(order.price)
            .or_insert_with(VecDeque::new)
            .push_back(order);
    }

    pub fn match_order(&mut self, mut taker: BookOrder) -> Vec<Trade> {
        let mut trades = Vec::new();

        loop {
            if taker.remaining_quantity() == Decimal::ZERO {
                break;
            }

            let best_opposite = match taker.side {
                OrderSide::Buy => self.asks.iter_mut().next(),
                OrderSide::Sell => self.bids.iter_mut().next_back(),
            };

            let (maker_price, maker_level) = match best_opposite {
                Some(entry) => entry,
                None => break,
            };

            let price_matches = match taker.order_type {
                OrderType::Market => true,
                OrderType::Limit => match taker.side {
                    OrderSide::Buy => taker.price >= *maker_price,
                    OrderSide::Sell => taker.price <= *maker_price,
                },
            };

            if !price_matches {
                break;
            }

            let maker = match maker_level.front_mut() {
                Some(o) => o,
                None => break,
            };

            let fill_qty = taker.remaining_quantity().min(maker.remaining_quantity());
            let fill_price = maker.price;

            taker.filled_quantity += fill_qty;
            maker.filled_quantity += fill_qty;

            if maker.remaining_quantity() == Decimal::ZERO {
                maker.status = OrderStatus::Filled;
            } else {
                maker.status = OrderStatus::PartiallyFilled;
            }

            if taker.remaining_quantity() == Decimal::ZERO {
                taker.status = OrderStatus::Filled;
            } else {
                taker.status = OrderStatus::PartiallyFilled;
            }

            let trade = Trade {
                id: Uuid::new_v4(),
                symbol: taker.symbol.clone(),
                maker_order_id: maker.id,
                taker_order_id: taker.id,
                maker_user_id: maker.user_id,
                taker_user_id: taker.user_id,
                price: fill_price,
                quantity: fill_qty,
                taker_side: match taker.side {
                    OrderSide::Buy => "BUY".to_string(),
                    OrderSide::Sell => "SELL".to_string(),
                },
                executed_at: Utc::now(),
            };

            trades.push(trade);

            if maker.status == OrderStatus::Filled {
                maker_level.pop_front();
            }

            if maker_level.is_empty() {
                let price_to_remove = *maker_price;
                let _ = maker_level;
                match taker.side {
                    OrderSide::Buy => self.asks.remove(&price_to_remove),
                    OrderSide::Sell => self.bids.remove(&price_to_remove),
                };
            }

            if taker.status == OrderStatus::Filled {
                break;
            }
        }

        if taker.status != OrderStatus::Filled && taker.order_type == OrderType::Limit {
            self.add_order(taker);
        }

        trades
    }

    pub fn cancel_order(&mut self, order_id: Uuid, side: &OrderSide) -> Option<(Decimal, Decimal)> {
        let book_side = match side {
            OrderSide::Buy => &mut self.bids,
            OrderSide::Sell => &mut self.asks,
        };

        let mut result = None;
        for level in book_side.values_mut() {
            if let Some(pos) = level.iter().position(|o| o.id == order_id) {
                if let Some(order) = level.remove(pos) {
                    result = Some((order.price, order.quantity));
                    break;
                }
            }
        }

        book_side.retain(|_, level| !level.is_empty());
        result
    }

    pub fn get_l2_depth(&self, levels: usize) -> (Vec<(Decimal, Decimal)>, Vec<(Decimal, Decimal)>) {
        let mut bids = Vec::new();
        for (price, level) in self.bids.iter().rev().take(levels) {
            let total_qty: Decimal = level.iter().map(|o| o.remaining_quantity()).sum();
            bids.push((*price, total_qty));
        }

        let mut asks = Vec::new();
        for (price, level) in self.asks.iter().take(levels) {
            let total_qty: Decimal = level.iter().map(|o| o.remaining_quantity()).sum();
            asks.push((*price, total_qty));
        }

        (bids, asks)
    }

}
