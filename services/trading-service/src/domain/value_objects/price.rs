use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq)]
pub struct Price(pub Decimal);

impl Price {
    pub fn new(value: Decimal) -> Self {
        Self(value)
    }

    pub fn value(&self) -> Decimal {
        self.0
    }
}