use std::collections::{BTreeMap, VecDeque};

use barter_instrument::{exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use derive_more::Constructor;
use rust_decimal::Decimal;

use crate::order::{Order, state::Open};

use super::price_key::PriceKey;

#[derive(Debug, Constructor)]
pub struct OrderBookSide<T>
where
    T: PriceKey + From<Decimal>,
{
    pub levels: BTreeMap<T, VecDeque<Order<ExchangeId, InstrumentNameExchange, Open>>>,
}

impl<T> OrderBookSide<T>
where
    T: PriceKey + From<Decimal>,
{
    pub fn insert_order(&mut self, order: &Order<ExchangeId, InstrumentNameExchange, Open>) {
        let key = T::from(order.price.clone());
        self.levels
            .entry(key)
            .or_insert_with(VecDeque::new)
            .push_back(order.clone());
    }

    pub fn is_order_fillable(
        &mut self,
        order: &Order<ExchangeId, InstrumentNameExchange, Open>,
    ) -> bool {
        self.levels
            .first_entry()
            .map(|entry| entry.key().fill_possible(order.price))
            .unwrap_or(false)
    }
}
