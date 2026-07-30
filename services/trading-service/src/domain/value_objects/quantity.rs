use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq)]
pub struct Quantity(pub Decimal);

impl Quantity {
    pub fn new(value: Decimal) -> Self {
        Self(value)
    }

    pub fn value(&self) -> Decimal {
        self.0
    }
}
