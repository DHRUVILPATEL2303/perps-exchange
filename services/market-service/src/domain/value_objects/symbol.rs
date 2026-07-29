#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Symbol(String);

impl Symbol {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into().trim().to_uppercase();

        if value.is_empty() {
            return Err(DomainError::InvalidSymbol);
        }

        if value.len() > 30 {
            return Err(DomainError::InvalidSymbol);
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}
