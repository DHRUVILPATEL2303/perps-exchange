use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::{
    enums::{
        margin_mode::MarginMode, order_side::OrderSide, order_status::OrderStatus,
        order_type::OrderType, time_in_force::TimeInForce,
    },
    value_objects::{
        client_order_id::ClientOrderId, levarage::Leverage, price::Price, quantity::Quantity,
    },
};

#[derive(Debug, Clone)]
pub struct Order {
    pub id: Uuid,
    pub client_order_id: ClientOrderId,
    pub user_id: Uuid,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub status: OrderStatus,
    pub time_in_force: TimeInForce,
    pub price: Option<Price>,
    pub quantity: Quantity,
    pub filled_quantity: Quantity,
    pub leverage: Leverage,
    pub reduce_only: bool,
    pub post_only: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub trigger_price: Option<Price>,
    pub average_fill_price: Option<Price>,
    pub margin_mode: MarginMode,
    pub close_position: bool,
    pub trade_ids: Vec<Uuid>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub rejected_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
}

impl Order {
    pub fn new(
        client_order_id: ClientOrderId,
        user_id: Uuid,
        symbol: String,
        side: OrderSide,
        order_type: OrderType,
        price: Option<Price>,
        quantity: Quantity,
        leverage: Leverage,
        tif: TimeInForce,
        trigger_price: Option<Price>,
        margin_mode: MarginMode,
        reduce_only: bool,
        post_only: bool,
        close_position: bool,
    ) -> Result<Self> {
        let now = Utc::now();

        if quantity.value() <= rust_decimal::Decimal::ZERO {
            bail!("Order quantity must be positive");
        }

        if leverage.value() == 0 {
            bail!("Leverage must be greater than zero");
        }

        if matches!(order_type, OrderType::Limit | OrderType::StopLimit) && price.is_none() {
            bail!("Limit price is required for Limit and StopLimit orders");
        }

        if let Some(ref p) = price {
            if p.value() <= rust_decimal::Decimal::ZERO {
                bail!("Limit price must be positive");
            }
        }

        if matches!(order_type, OrderType::StopMarket | OrderType::StopLimit)
            && trigger_price.is_none()
        {
            bail!("Trigger price is required for StopMarket and StopLimit orders");
        }

        if let Some(ref tp) = trigger_price {
            if tp.value() <= rust_decimal::Decimal::ZERO {
                bail!("Trigger price must be positive");
            }
        }

        if post_only && !matches!(order_type, OrderType::Limit | OrderType::StopLimit) {
            bail!("Post-only option is only valid for Limit or StopLimit orders");
        }

        if tif == TimeInForce::GTX && !matches!(order_type, OrderType::Limit | OrderType::StopLimit)
        {
            bail!("GTX (Post-Only) time in force is only valid for Limit or StopLimit orders");
        }

        Ok(Self {
            id: Uuid::new_v4(),            client_order_id,
            user_id,
            symbol,
            side,
            order_type,
            status: OrderStatus::New,
            time_in_force: tif,
            price,
            quantity,
            filled_quantity: Quantity::new(rust_decimal::Decimal::ZERO),
            leverage,
            reduce_only,
            post_only,
            created_at: now,
            updated_at: now,
            trigger_price,
            average_fill_price: None,
            margin_mode,
            close_position,
            trade_ids: Vec::new(),
            cancelled_at: None,
            rejected_at: None,
            expired_at: None,
        })
    }

    pub fn remaining_quantity(&self) -> Quantity {
        Quantity::new(self.quantity.value() - self.filled_quantity.value())
    }

    pub fn is_filled(&self) -> bool {
        self.remaining_quantity().value().is_zero()
    }

    pub fn is_partially_filled(&self) -> bool {
        self.filled_quantity.value() > rust_decimal::Decimal::ZERO && !self.is_filled()
    }

    pub fn is_market(&self) -> bool {
        matches!(self.order_type, OrderType::Market)
    }

    pub fn is_limit(&self) -> bool {
        matches!(self.order_type, OrderType::Limit)
    }

    pub fn is_buy(&self) -> bool {
        matches!(self.side, OrderSide::Buy)
    }

    pub fn is_sell(&self) -> bool {
        matches!(self.side, OrderSide::Sell)
    }

    pub fn cancel(&mut self) -> Result<()> {
        if !self.is_active() {
            bail!("Cannot cancel inactive order (status: {:?})", self.status);
        }
        let now = Utc::now();
        self.status = OrderStatus::Cancelled;
        self.cancelled_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    pub fn reject(&mut self) -> Result<()> {
        if self.status != OrderStatus::New {
            bail!(
                "Cannot reject order that is already processed (status: {:?})",
                self.status
            );
        }
        let now = Utc::now();
        self.status = OrderStatus::Rejected;
        self.rejected_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    pub fn expire(&mut self) -> Result<()> {
        if !self.is_active() {
            bail!("Cannot expire inactive order (status: {:?})", self.status);
        }
        let now = Utc::now();
        self.status = OrderStatus::Expired;
        self.expired_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        matches!(self.status, OrderStatus::New | OrderStatus::PartiallyFilled)
    }

    pub fn can_match(&self) -> bool {
        self.is_active()
    }

    pub fn fill(&mut self, qty: Quantity, fill_price: Price, trade_id: Uuid) -> Result<()> {
        if qty.value() <= rust_decimal::Decimal::ZERO {
            bail!("fill quantity must be positive");
        }

        if fill_price.value() <= rust_decimal::Decimal::ZERO {
            bail!("fill price must be positive");
        }

        if qty.value() > self.remaining_quantity().value() {
            bail!("fill exceeds remaining quantity");
        }

        if !self.is_active() {
            bail!("Cannot fill inactive order (status: {:?})", self.status);
        }

        let prev_filled = self.filled_quantity.value();
        let new_filled = prev_filled + qty.value();

        let prev_avg = self
            .average_fill_price
            .as_ref()
            .map(|p| p.value())
            .unwrap_or(rust_decimal::Decimal::ZERO);

        let new_avg = (prev_avg * prev_filled + fill_price.value() * qty.value()) / new_filled;

        self.filled_quantity = Quantity::new(new_filled);
        self.average_fill_price = Some(Price::new(new_avg));
        self.trade_ids.push(trade_id);

        self.updated_at = Utc::now();

        if self.is_filled() {
            self.status = OrderStatus::Filled;
        } else {
            self.status = OrderStatus::PartiallyFilled;
        }

        Ok(())
    }
}
