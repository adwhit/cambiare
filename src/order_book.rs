use std::collections::{btree_map::Entry, BTreeMap};

use crossbeam_channel::{Receiver, Sender};

use crate::{Balance, OrderId, Price, Volume};

const LEVEL_QUOTE_INIT_CAPACITY: usize = 128;
const TOMBSTONE_GC_LIMIT: u32 = 1000;

#[derive(Clone, Debug)]
pub(crate) struct Level {
    total_volume: Volume,
    quotes: Vec<Quote>,
    tombstone_count: u32,
}

impl Level {
    pub(crate) fn total_volume(&self) -> Volume {
        self.total_volume
    }
    fn iter_quotes(&self) -> impl Iterator<Item = &Quote> {
        self.quotes.iter().filter(|q| !q.is_tombstone())
    }
    fn iter_quotes_mut(&mut self) -> impl Iterator<Item = &mut Quote> {
        self.quotes.iter_mut().filter(|q| !q.is_tombstone())
    }
    fn compact(&mut self) {
        self.quotes.retain(|q| !q.is_tombstone());
        self.tombstone_count = 0;
    }

    fn maybe_compact(&mut self) {
        if self.tombstone_count >= TOMBSTONE_GC_LIMIT {
            self.compact();
        }
    }
    fn clear(&mut self) {
        self.total_volume = Volume::new(0);
        self.quotes.clear();
        self.tombstone_count = 0;
    }
}

impl Default for Level {
    fn default() -> Self {
        Self {
            total_volume: Volume::new(0),
            quotes: Vec::with_capacity(LEVEL_QUOTE_INIT_CAPACITY),
            tombstone_count: 0,
        }
    }
}

#[derive(Copy, Clone, derive_more::Constructor)]
pub struct Quote {
    order_id: OrderId,
    volume: Volume,
}

impl std::fmt::Debug for Quote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_tombstone() {
            write!(f, "TOMBSTONE")
        } else {
            write!(f, "Q({:?} -> {:?})", self.order_id, self.volume)
        }
    }
}

impl Quote {
    fn tombstone() -> Quote {
        Quote {
            order_id: OrderId::new(u64::MAX),
            volume: Volume::new(u64::MAX),
        }
    }

    fn is_tombstone(&self) -> bool {
        self.order_id == OrderId::new(u64::MAX)
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum MatchType {
    MakerFilled,
    TakerFilled,
    BothFilled,
}

#[derive(Copy, Clone, PartialEq, Eq, derive_more::Constructor)]
pub struct Match {
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub price: Price,
    pub volume: Volume,
    pub typ: MatchType,
}

impl std::fmt::Debug for Match {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (mf, tf) = match self.typ {
            MatchType::MakerFilled => ("F", " "),
            MatchType::TakerFilled => (" ", "F"),
            MatchType::BothFilled => ("F", "F"),
        };
        write!(
            f,
            "Match({:?}{mf} <-> {:?}{tf} {}@{})",
            self.maker_order_id, self.taker_order_id, self.volume, self.price
        )
    }
}

#[derive(Clone)]
pub struct OrderBook {
    best_ask: Price,
    best_bid: Price,
    levels: BTreeMap<Price, Level>,
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderBook {
    pub fn new() -> Self {
        OrderBook {
            best_ask: Price::new(u64::MAX),
            best_bid: Price::new(u64::MIN),
            levels: BTreeMap::new(),
        }
    }

    pub(crate) fn ask_levels(&self) -> impl Iterator<Item = (&Price, &Level)> {
        self.levels.range(self.best_ask..)
    }

    fn ask_levels_mut(&mut self) -> impl Iterator<Item = (&Price, &mut Level)> {
        self.levels.range_mut(self.best_ask..)
    }

    pub(crate) fn bid_levels(&self) -> impl Iterator<Item = (&Price, &Level)> {
        self.levels.range(..=self.best_bid).rev()
    }

    fn bid_levels_mut(&mut self) -> impl Iterator<Item = (&Price, &mut Level)> {
        self.levels.range_mut(..=self.best_bid).rev()
    }

    pub fn ask_volume(&self) -> Volume {
        self.ask_levels()
            .fold(Volume::new(0), |acc, (_, lvl)| acc + lvl.total_volume)
    }

    pub fn best_bid(&self) -> Price {
        self.best_bid
    }

    pub fn best_ask(&self) -> Price {
        self.best_ask
    }

    pub fn spread(&self) -> Price {
        self.best_ask - self.best_bid
    }

    pub fn add_bid(&mut self, price: Price, quote: Quote) -> Outcome {
        if self.best_ask <= price {
            return Outcome::CrossedSpread;
        }
        let did_update;
        match self.levels.entry(price) {
            // new level
            Entry::Vacant(v) => {
                did_update = false;
                let level = Level {
                    total_volume: quote.volume,
                    quotes: vec![quote],
                    tombstone_count: 0,
                };
                v.insert(level);
            }
            // existing level
            Entry::Occupied(mut o) => {
                let level = o.get_mut();
                did_update = level.total_volume != Volume::new(0);
                level.total_volume += quote.volume;
                level.quotes.push(quote);
            }
        }
        if self.best_bid < price {
            self.best_bid = price;
            return Outcome::PlacedNewBest;
        }
        if did_update {
            Outcome::PlacedExisting
        } else {
            Outcome::PlacedNew
        }
    }

    pub fn add_ask(&mut self, price: Price, quote: Quote) -> Outcome {
        if self.best_bid >= price {
            return Outcome::CrossedSpread;
        }
        let did_update;
        match self.levels.entry(price) {
            Entry::Vacant(v) => {
                did_update = false;
                let level = Level {
                    total_volume: quote.volume,
                    quotes: vec![quote],
                    tombstone_count: 0,
                };
                v.insert(level);
            }
            Entry::Occupied(mut o) => {
                let level = o.get_mut();
                did_update = level.total_volume != Volume::new(0);
                level.total_volume += quote.volume;
                level.quotes.push(quote);
            }
        }
        if self.best_ask > price {
            self.best_ask = price;
            return Outcome::PlacedNewBest;
        }
        if did_update {
            Outcome::PlacedExisting
        } else {
            Outcome::PlacedNew
        }
    }

    fn cancel(&mut self, price: Price, order_id: OrderId) -> Cancellation {
        let Some(level) = self.levels.get_mut(&price) else {
            return Cancellation::NotFound;
        };
        for q in level.quotes.iter_mut() {
            if q.order_id == order_id {
                level.total_volume -= q.volume;
                *q = Quote::tombstone();
                level.tombstone_count += 1;
                level.maybe_compact();
                return Cancellation::WasCancelled;
            }
        }
        Cancellation::NotFound
    }

    pub fn execute_market_buy(
        &mut self,
        order_id: OrderId,
        target_vol: Volume,
        available_quote_balance: Balance,
        fills: &mut Vec<Match>,
    ) -> TxnOutcome {
        {
            // first validate that the transaction is possible
            let mut rem_bal = available_quote_balance;
            let mut rem_vol = target_vol;
            for (price, level) in self.ask_levels() {
                let vol = std::cmp::min(rem_vol, level.total_volume);
                if price.inner() * vol.inner() > rem_bal.inner() {
                    // oh dear, not enough funds to complete
                    return TxnOutcome::FailedInsufficientFunds;
                }
                if rem_vol < level.total_volume {
                    break;
                }
                rem_vol -= level.total_volume;
                rem_bal -= Balance::new(price.inner() * level.total_volume.inner());
            }
        }

        let res = execute_market_txn(
            self.ask_levels_mut(),
            order_id,
            target_vol,
            OrderTarget::MarketBuy {
                available_quote_balance,
            },
            fills,
        );
        if let TxnOutcome::Filled { new_best_price } = res {
            self.best_ask = new_best_price
        } else {
            self.best_ask = Price::new(u64::MAX);
        };
        res
    }

    pub fn execute_market_sell(
        &mut self,
        order_id: OrderId,
        target_vol: Volume,
        fills: &mut Vec<Match>,
    ) -> TxnOutcome {
        let res = execute_market_txn(
            self.bid_levels_mut(),
            order_id,
            target_vol,
            OrderTarget::MarketSell,
            fills,
        );
        if let TxnOutcome::Filled { new_best_price } = res {
            self.best_bid = new_best_price
        } else {
            self.best_bid = Price::new(u64::MIN);
        };
        res
    }

    pub fn execute_limit_buy_order(
        &mut self,
        order_id: OrderId,
        target_price: Price,
        target_vol: Volume,
        fills: &mut Vec<Match>,
    ) {
        // may fill or partially fill
        let res = execute_market_txn(
            self.ask_levels_mut(),
            order_id,
            target_vol,
            OrderTarget::LimitBuy(target_price),
            fills,
        );
        match res {
            TxnOutcome::Filled { new_best_price } => {
                self.best_ask = new_best_price;
            }
            TxnOutcome::PartiallyFilled {
                volume_transacted,
                new_best_price,
            } => {
                self.best_ask = new_best_price;
                self.add_bid(
                    target_price,
                    Quote::new(order_id, target_vol - volume_transacted),
                )
                .assert_placed();
            }
            TxnOutcome::MarketVolumeExhausted { volume_transacted } => {
                self.best_ask = Price::new(u64::MAX);
                self.add_bid(
                    target_price,
                    Quote::new(order_id, target_vol - volume_transacted),
                )
                .assert_placed();
            }
            TxnOutcome::FailedInsufficientFunds => unreachable!(),
        }
    }
    pub fn execute_limit_sell_order(
        &mut self,
        order_id: OrderId,
        target_price: Price,
        target_vol: Volume,
        fills: &mut Vec<Match>,
    ) {
        // may fill or partially fill
        let res = execute_market_txn(
            self.bid_levels_mut(),
            order_id,
            target_vol,
            OrderTarget::LimitSell(target_price),
            fills,
        );
        match res {
            TxnOutcome::Filled { new_best_price } => {
                self.best_bid = new_best_price;
            }
            TxnOutcome::PartiallyFilled {
                volume_transacted,
                new_best_price,
            } => {
                self.best_bid = new_best_price;
                self.add_ask(
                    target_price,
                    Quote::new(order_id, target_vol - volume_transacted),
                )
                .assert_placed();
            }
            TxnOutcome::MarketVolumeExhausted { volume_transacted } => {
                self.best_bid = Price::new(u64::MIN);
                self.add_ask(
                    target_price,
                    Quote::new(order_id, target_vol - volume_transacted),
                )
                .assert_placed();
            }
            TxnOutcome::FailedInsufficientFunds => unreachable!(),
        }
    }
}

enum Cancellation {
    WasCancelled,
    NotFound,
}

#[derive(PartialEq, Eq, Debug)]
pub enum TxnOutcome {
    Filled {
        new_best_price: Price,
    },
    PartiallyFilled {
        volume_transacted: Volume,
        new_best_price: Price,
    },
    MarketVolumeExhausted {
        volume_transacted: Volume,
    },
    FailedInsufficientFunds,
}

impl TxnOutcome {
    // test helper
    pub fn filled(self) -> Price {
        if let TxnOutcome::Filled { new_best_price } = self {
            return new_best_price;
        }
        panic!("expected Filled, got: {self:?}")
    }
    // test helper
    pub fn partial(self) -> (Price, Volume) {
        if let TxnOutcome::PartiallyFilled {
            new_best_price,
            volume_transacted,
        } = self
        {
            return (new_best_price, volume_transacted);
        }
        panic!("expected PartiallyFilled, got: {self:?}")
    }
    // test helper
    pub fn exhausted(self) -> Volume {
        if let TxnOutcome::MarketVolumeExhausted { volume_transacted } = self {
            return volume_transacted;
        }
        panic!("expected MarketVolumeExhausted, got: {self:?}")
    }
    // test helper
    pub fn failed(self) {
        if let TxnOutcome::FailedInsufficientFunds = self {
            return;
        }
        panic!("expected FailedInsufficientFunds, got: {self:?}")
    }
}

enum OrderTarget {
    LimitBuy(Price),
    LimitSell(Price),
    MarketBuy { available_quote_balance: Balance },
    MarketSell,
}

fn execute_market_txn<'a>(
    price_levels: impl Iterator<Item = (&'a Price, &'a mut Level)>,
    order_id: OrderId,
    target_vol: Volume,
    target_price: OrderTarget,
    matches: &mut Vec<Match>,
) -> TxnOutcome {
    let mut remaining_txn_vol = target_vol;
    for (&price, level) in price_levels {
        match target_price {
            OrderTarget::LimitBuy(max_buy_price) => {
                if max_buy_price < price {
                    // we're done
                    return TxnOutcome::PartiallyFilled {
                        volume_transacted: target_vol - remaining_txn_vol,
                        new_best_price: price,
                    };
                }
            }
            OrderTarget::LimitSell(min_sell_price) => {
                if min_sell_price > price {
                    // we're done
                    return TxnOutcome::PartiallyFilled {
                        volume_transacted: target_vol - remaining_txn_vol,
                        new_best_price: price,
                    };
                }
            }
            OrderTarget::MarketBuy { .. } | OrderTarget::MarketSell => {
                // no problem
            }
        }
        if remaining_txn_vol == Volume::new(0) {
            // we're done
            return TxnOutcome::Filled {
                new_best_price: price,
            };
        } else if remaining_txn_vol >= level.total_volume {
            // will exhaust this level
            remaining_txn_vol -= level.total_volume;
            for q in level.iter_quotes() {
                let matchty = if remaining_txn_vol == Volume::new(0) {
                    MatchType::BothFilled
                } else {
                    MatchType::MakerFilled
                };
                matches.push(Match::new(q.order_id, order_id, price, q.volume, matchty));
            }
            level.clear();
            // continue to next price level
        } else {
            // will end at this level
            level.total_volume -= remaining_txn_vol;

            let mut tombstone_inc = 0;
            for q in level.iter_quotes_mut() {
                if remaining_txn_vol < q.volume {
                    // taker filled (and we're done)
                    q.volume -= remaining_txn_vol;
                    matches.push(Match::new(
                        q.order_id,
                        order_id,
                        price,
                        remaining_txn_vol,
                        MatchType::TakerFilled,
                    ));
                    break;
                } else if remaining_txn_vol == q.volume {
                    // both filled (and we're done)
                    matches.push(Match::new(
                        q.order_id,
                        order_id,
                        price,
                        remaining_txn_vol,
                        MatchType::BothFilled,
                    ));
                    *q = Quote::tombstone();
                    break;
                } else {
                    // maker filled (and continue)
                    remaining_txn_vol -= q.volume;
                    matches.push(Match::new(
                        q.order_id,
                        order_id,
                        price,
                        q.volume,
                        MatchType::MakerFilled,
                    ));
                    *q = Quote::tombstone();
                    tombstone_inc += 1;
                }
            }
            level.tombstone_count += tombstone_inc;
            level.maybe_compact();
            // we're done
            return TxnOutcome::Filled {
                new_best_price: price,
            };
        }
    }
    // if we get here then we used up all the volume
    TxnOutcome::MarketVolumeExhausted {
        volume_transacted: target_vol - remaining_txn_vol,
    }
}

pub enum Outcome {
    PlacedExisting,
    PlacedNew,
    PlacedNewBest,
    CrossedSpread,
}

impl Outcome {
    fn assert_placed(self) {
        match self {
            Outcome::PlacedExisting | Outcome::PlacedNew | Outcome::PlacedNewBest => {}
            Outcome::CrossedSpread => panic!("order was not placed"),
        }
    }
}

pub enum OrderType {
    // buy some quantity of base
    // GBPUSD -> 'buy 1000GBP, use whatever USD I have in my account'
    MarketBuy {
        target_base_qty: Volume,
        available_quote_balance: Balance,
    },
    // buy the base, until we have sold some target quote amount
    // GBPUSD -> 'buy 1000USD-worth of GBP'
    MarketBuyQ {
        target_quote_balance: Balance,
    },
    // sell a quantity of the base
    // GBPUSD -> 'sell 1000GBP'
    MarketSell {
        base_qty: Volume,
    },
    // sell a quantify of the base, until we have bought some target quote amount
    // GBPUSD -> 'sell 1000USD-worth of GBP'
    MarketSellQ {
        target_quote_balance: Balance,
        available_base_qty: Volume,
    },
    LimitBuy {
        price: Price,
        volume: Volume,
    },
    LimitSell {
        price: Price,
        volume: Volume,
    },
    Cancel {
        price: Price,
        order_id: OrderId,
    },
    /// Take a copy of the order book and send back
    /// along the snapshot channel
    SendSnapshot,
}

pub struct Order {
    pub id: OrderId,
    pub typ: OrderType,
}

pub fn run_orderbook_event_loop(
    order_rx: Receiver<Order>,
    match_tx: Sender<Match>,
    snapshot_tx: Sender<OrderBook>,
) {
    let mut book = OrderBook::new();
    let mut matches_buffer = Vec::with_capacity(1000);
    loop {
        let order = order_rx.recv().unwrap();
        match order.typ {
            OrderType::MarketBuy {
                target_base_qty,
                available_quote_balance,
            } => {
                book.execute_market_buy(
                    order.id,
                    target_base_qty,
                    available_quote_balance,
                    &mut matches_buffer,
                );
            }

            OrderType::MarketSell { base_qty } => {
                book.execute_market_sell(order.id, base_qty, &mut matches_buffer);
            }
            OrderType::MarketBuyQ {
                target_quote_balance,
            } => todo!(),
            OrderType::MarketSellQ {
                target_quote_balance,
                available_base_qty,
            } => todo!(),
            OrderType::LimitBuy { price, volume } => {
                book.execute_limit_buy_order(order.id, price, volume, &mut matches_buffer)
            }
            OrderType::LimitSell { price, volume } => {
                book.execute_limit_sell_order(order.id, price, volume, &mut matches_buffer)
            }
            OrderType::Cancel { price, order_id } => {
                match book.cancel(price, order_id) {
                    Cancellation::WasCancelled => {}
                    Cancellation::NotFound => todo!(),
                };
                continue;
            }
            OrderType::SendSnapshot => snapshot_tx.send(book.clone()).unwrap(),
        }
        for &fill in matches_buffer.iter() {
            match_tx.send(fill).expect("tx_fill send failed");
        }
        matches_buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    fn b(v: u64) -> Balance {
        Balance::new(v)
    }
    fn p(v: u64) -> Price {
        Price::new(v)
    }
    fn v(v: u64) -> Volume {
        Volume::new(v)
    }
    fn o(v: u64) -> OrderId {
        OrderId::new(v)
    }
    fn mm(maker: u64, taker: u64, price: u64, vol: u64) -> Match {
        Match::new(o(maker), o(taker), p(price), v(vol), MatchType::MakerFilled)
    }
    fn mt(maker: u64, taker: u64, price: u64, vol: u64) -> Match {
        Match::new(o(maker), o(taker), p(price), v(vol), MatchType::TakerFilled)
    }
    fn mb(maker: u64, taker: u64, price: u64, vol: u64) -> Match {
        Match::new(o(maker), o(taker), p(price), v(vol), MatchType::BothFilled)
    }
    fn q(q: u64, v: u64) -> Quote {
        Quote {
            order_id: OrderId::new(q),
            volume: Volume::new(v),
        }
    }
    fn olb(id: u64, price: u64, vol: u64) -> Order {
        Order {
            id: o(id),
            typ: OrderType::LimitBuy {
                price: p(price),
                volume: v(vol),
            },
        }
    }
    fn ols(id: u64, price: u64, vol: u64) -> Order {
        Order {
            id: o(id),
            typ: OrderType::LimitSell {
                price: p(price),
                volume: v(vol),
            },
        }
    }
    fn omb(id: u64, vol: u64) -> Order {
        Order {
            id: o(id),
            typ: OrderType::MarketBuy {
                target_base_qty: v(vol),
                available_quote_balance: b(1_000_000),
            },
        }
    }
    fn oms(id: u64, vol: u64) -> Order {
        Order {
            id: o(id),
            typ: OrderType::MarketSell { base_qty: v(vol) },
        }
    }

    fn quick_book() -> OrderBook {
        let mut ob = OrderBook::new();
        ob.add_bid(p(10), q(1, 40));
        ob.add_bid(p(15), q(2, 30));
        ob.add_bid(p(20), q(3, 20));
        ob.add_bid(p(25), q(4, 10));

        ob.add_ask(p(35), q(5, 10));
        ob.add_ask(p(40), q(6, 20));
        ob.add_ask(p(45), q(7, 30));
        ob.add_ask(p(50), q(8, 40));
        ob
    }

    #[test]
    fn test_insert_bids_asks_simple() {
        let mut ob = OrderBook::new();
        assert!(matches!(
            ob.add_bid(p(100), q(1, 200)),
            Outcome::PlacedNewBest
        ));
        assert!(matches!(ob.add_bid(p(50), q(2, 100)), Outcome::PlacedNew));
        assert!(matches!(
            ob.add_bid(p(100), q(3, 300)),
            Outcome::PlacedExisting
        ));
        assert!(matches!(
            ob.add_ask(p(101), q(4, 300)),
            Outcome::PlacedNewBest
        ));
        assert!(matches!(
            ob.add_ask(p(101), q(5, 300)),
            Outcome::PlacedExisting
        ));
        assert_eq!(ob.spread(), p(1));
        assert!(matches!(
            ob.add_bid(p(101), q(6, 100)),
            Outcome::CrossedSpread
        ));
        assert!(matches!(
            ob.add_ask(p(100), q(7, 100)),
            Outcome::CrossedSpread
        ));
    }

    #[test]
    fn test_execute_market_buy() {
        let mut ob = quick_book();

        assert_eq!(ob.spread(), p(10));

        {
            let mut matches = Vec::new();
            ob.execute_market_buy(o(100), v(1), b(10000), &mut matches)
                .filled();
            let expect_fills = &[mt(5, 100, 35, 1)];
            assert_eq!(matches, expect_fills);
            assert_eq!(ob.best_ask(), p(35));
        }
        {
            let mut matches = Vec::new();
            ob.execute_market_buy(o(101), v(24), b(10000), &mut matches)
                .filled();
            let expect_fills = &[mm(5, 101, 35, 9), mt(6, 101, 40, 15)];
            assert_eq!(matches, expect_fills);
            assert_eq!(ob.best_ask(), p(40));
        }
        {
            let mut matches = Vec::new();
            ob.execute_market_buy(o(102), v(5), b(10000), &mut matches)
                .filled();
            let expect_fills = &[mb(6, 102, 40, 5)];
            assert_eq!(matches, expect_fills);
            assert_eq!(ob.best_ask(), p(45));
        }
        {
            let mut matches = Vec::new();
            let filled_vol = ob
                .execute_market_buy(o(103), v(500), b(10000), &mut matches)
                .exhausted();
            assert_eq!(filled_vol, v(70));
            let expect_fills = &[mm(7, 103, 45, 30), mm(8, 103, 50, 40)];
            assert_eq!(matches, expect_fills);
            assert_eq!(ob.best_ask(), p(u64::MAX));
        }
    }

    #[test]
    fn test_execute_market_partial() {
        let mut book = OrderBook::new();
        book.add_ask(p(10), q(1, 10));
        book.add_ask(p(10), q(2, 10));
        book.add_ask(p(10), q(3, 10));

        {
            let mut fills = Vec::new();
            let res = book.execute_market_buy(o(100), v(11), b(10000), &mut fills);
            assert_eq!(
                res,
                TxnOutcome::Filled {
                    new_best_price: p(10)
                }
            );
            let expect_fills = &[mm(1, 100, 10, 10), mt(2, 100, 10, 1)];
            assert_eq!(fills, expect_fills);
            assert_eq!(book.ask_volume(), v(19));
        }
        {
            let mut fills = Vec::new();
            let res = book.execute_market_buy(o(101), v(9), b(10000), &mut fills);
            assert_eq!(
                res,
                TxnOutcome::Filled {
                    new_best_price: p(10)
                }
            );
            let expect_fills = &[mb(2, 101, 10, 9)];
            assert_eq!(fills, expect_fills);
        }
    }

    #[test]
    fn test_execute_market_sell_simple() {
        let mut ob = quick_book();
        {
            let mut matches = Vec::new();
            let res = ob.execute_market_sell(o(100), v(90), &mut matches);
            assert_eq!(res.filled(), p(10));
            assert_eq!(
                matches,
                &[
                    mm(4, 100, 25, 10),
                    mm(3, 100, 20, 20),
                    mm(2, 100, 15, 30),
                    mt(1, 100, 10, 30)
                ]
            )
        }
        {
            let mut fills = Vec::new();
            let res = ob.execute_market_sell(o(101), v(22), &mut fills);
            assert_eq!(res.exhausted(), v(10));
            assert_eq!(fills, &[mm(1, 101, 10, 10)])
        }
    }

    #[test]
    fn test_zero_volume_scenarios() {
        let mut book = OrderBook::new();
        let mut matches = Vec::new();
        {
            // TODO a zero-volume order should probably return success?
            book.execute_market_buy(o(100), v(0), b(10000), &mut matches)
                .exhausted();
            book.execute_market_sell(o(101), v(0), &mut matches)
                .exhausted();
        }
        {
            book.execute_market_buy(o(102), v(10), b(10000), &mut matches)
                .exhausted();
            book.execute_market_sell(o(103), v(10), &mut matches)
                .exhausted();
        }
        {
            book.add_ask(p(20), q(1, 10));
            book.add_bid(p(10), q(1, 10));
            book.execute_market_buy(o(104), v(0), b(10000), &mut matches)
                .filled();
            book.execute_market_sell(o(105), v(0), &mut matches)
                .filled();
        }
    }

    #[test]
    fn test_order_cancellation() {
        let mut book = quick_book();
        assert!(matches!(
            book.cancel(p(15), o(2)),
            Cancellation::WasCancelled
        ));
        assert!(matches!(book.cancel(p(15), o(2)), Cancellation::NotFound));
        assert!(matches!(book.cancel(p(20), o(222)), Cancellation::NotFound));
        assert!(matches!(
            book.cancel(p(35), o(5)),
            Cancellation::WasCancelled
        ));
        assert_eq!(book.ask_volume(), v(90));
        let mut fills = Vec::new();
        book.execute_market_buy(o(100), v(1), b(10000), &mut fills)
            .filled();
        assert_eq!(fills, &[mt(6, 100, 40, 1)]);
    }

    #[test]
    fn test_compactify() {
        let mut book = quick_book();
        for id in 0..TOMBSTONE_GC_LIMIT - 1 {
            // build up a load of tombstones
            book.add_ask(p(30), q((id + 20) as u64, 10));
            book.cancel(p(30), o((id + 20) as u64));
        }
        {
            let level = book.levels.get(&p(30)).unwrap();
            assert_eq!(level.quotes.len() as u32, TOMBSTONE_GC_LIMIT - 1);
            assert_eq!(level.tombstone_count, TOMBSTONE_GC_LIMIT - 1);
        }
        // trigger a compactification
        book.add_ask(p(30), q(333333, 10));
        book.cancel(p(30), o(333333));
        {
            let level = book.levels.get(&p(30)).unwrap();
            assert_eq!(level.quotes.len(), 0);
            assert_eq!(level.tombstone_count, 0);
        }
    }

    #[test]
    fn test_simple_limit_buy() {
        let mut book = quick_book();
        {
            let mut fills = Vec::new();
            book.execute_limit_buy_order(o(100), p(38), v(50), &mut fills);
            assert_eq!(fills, &[mm(5, 100, 35, 10)]);
            assert_eq!(book.best_bid(), p(38));
            assert_eq!(book.best_ask(), p(40));
        }
        {
            let mut fills = Vec::new();
            book.execute_limit_buy_order(o(101), p(40), v(1), &mut fills);
            assert_eq!(fills, &[mt(6, 101, 40, 1)])
        }
    }

    #[test]
    fn test_simple_limit_sell() {
        let mut book = quick_book();
        {
            let mut matches = Vec::new();
            book.execute_limit_sell_order(o(100), p(22), v(50), &mut matches);
            assert_eq!(matches, &[mm(4, 100, 25, 10)]);
            assert_eq!(book.best_bid(), p(20));
            assert_eq!(book.best_ask(), p(22));
        }
        {
            let mut matches = Vec::new();
            book.execute_limit_sell_order(o(101), p(20), v(1), &mut matches);
            assert_eq!(matches, &[mt(3, 101, 20, 1)])
        }
    }

    #[test]
    fn test_market_buy_balance_limited() {
        let mut book = quick_book();
        let mut matches = Vec::new();
        book.execute_market_buy(o(101), v(10), b(1), &mut matches)
            .failed();
        book.execute_market_buy(o(102), v(10), b(349), &mut matches)
            .failed();
        book.execute_market_buy(o(103), v(10), b(350), &mut matches)
            .filled();
        // 20 * 40 + 5 * 45 = 1025
        book.execute_market_buy(o(104), v(25), b(1000), &mut matches)
            .failed();
        book.execute_market_buy(o(105), v(25), b(1050), &mut matches)
            .filled();
        // finish rest
        book.execute_market_buy(o(106), v(500), b(10000), &mut matches)
            .exhausted();
    }

    #[test]
    fn test_run_order_book() {
        let (tx_order, rx_order) = crossbeam_channel::bounded(1000);
        let (tx_match, rx_match) = crossbeam_channel::bounded(1000);
        let (tx_snapshot, _rx_snapshot) = crossbeam_channel::bounded(1000);
        std::thread::spawn(move || run_orderbook_event_loop(rx_order, tx_match, tx_snapshot));

        // add three limit orders
        tx_order.send(olb(101, 10, 10)).unwrap();
        tx_order.send(olb(102, 9, 20)).unwrap();
        tx_order.send(olb(103, 8, 30)).unwrap();
        {
            // market sell
            tx_order.send(oms(104, 31)).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(30));

            let f1 = rx_match.recv_timeout(Duration::from_secs(1)).unwrap();
            let f2 = rx_match.try_recv().unwrap();
            let f3 = rx_match.try_recv().unwrap();
            assert_eq!(f1, mm(101, 104, 10, 10));
            assert_eq!(f2, mm(102, 104, 9, 20));
            assert_eq!(f3, mt(103, 104, 8, 1));
            assert!(rx_match.try_recv().is_err());
        }

        {
            // limit sell
            tx_order.send(ols(201, 5, 100)).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(30));

            let f1 = rx_match.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(f1, mm(103, 201, 8, 29));
            assert!(rx_match.try_recv().is_err());
        }

        {
            // market buy
            tx_order.send(omb(301, 200)).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(30));

            let f1 = rx_match.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(f1, mm(201, 301, 5, 71));
            assert!(rx_match.try_recv().is_err());
        }
    }
}
