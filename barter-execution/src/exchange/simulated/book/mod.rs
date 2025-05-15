use derive_more::Constructor;
use order_book_side::OrderBookSide;
use price_key::{AskPrice, BidPrice};

pub mod order_book_side;
pub mod price_key;

#[derive(Debug, Constructor)]
pub struct OrderBook {
    bid: OrderBookSide<BidPrice>,
    ask: OrderBookSide<AskPrice>,
}
