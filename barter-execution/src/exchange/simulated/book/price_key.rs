use rust_decimal::Decimal;
use std::cmp::Ordering;
use std::fmt::Debug;

pub trait PriceKey: Ord + Debug + Copy {
    fn fill_possible(&self, price: Decimal) -> bool;
    fn price(&self) -> Decimal;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AskPrice {
    price: Decimal,
}

impl PriceKey for AskPrice {
    fn fill_possible(&self, price: Decimal) -> bool {
        self.price <= price || price.is_zero()
    }

    fn price(&self) -> Decimal {
        self.price
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct BidPrice {
    price: Decimal,
}

impl Ord for BidPrice {
    fn cmp(&self, other: &Self) -> Ordering {
        other.price.cmp(&self.price)
    }
}

impl PartialOrd for BidPrice {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PriceKey for BidPrice {
    fn fill_possible(&self, price: Decimal) -> bool {
        self.price >= price || price.is_zero()
    }

    fn price(&self) -> Decimal {
        self.price
    }
}

impl From<Decimal> for AskPrice {
    fn from(price: Decimal) -> Self {
        AskPrice { price }
    }
}

impl From<Decimal> for BidPrice {
    fn from(price: Decimal) -> Self {
        BidPrice { price }
    }
}
