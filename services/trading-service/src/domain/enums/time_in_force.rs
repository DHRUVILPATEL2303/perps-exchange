#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce {
    GTC,
    IOC,
    FOK,
    GTX,
}