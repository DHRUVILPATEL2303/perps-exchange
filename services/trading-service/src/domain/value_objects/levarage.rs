#[derive(Debug, Clone, Copy)]
pub struct Leverage(pub u32);

impl Leverage {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u32 {
        self.0
    }
}
