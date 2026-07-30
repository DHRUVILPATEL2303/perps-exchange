#[derive(Debug, Clone)]
pub struct ClientOrderId(pub String);

impl ClientOrderId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}
