use std::collections::{btree_map::Entry, BTreeMap};

use crossbeam_channel::{Receiver, Sender};

use crate::{Balance, OrderId, Price, Volume};

const LEVEL_QUOTE_INIT_CAPACITY: usize = 128;
const TOMBSTONE_GC_LIMIT: u32 = 1000;

#[derive(Clone, Debug)]
struct Level {
    total_volume: Volume,
    quotes: Vec<Quote>,
    tombstone_count: u32,
}

impl Level {
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

#[derive(Copy, Clone, PartialEq, Eq, derive_more::Constructor)]
pub struct Fill {
    order_id: OrderId,
    completion: FillCompletion,
}

impl std::fmt::Debug for Fill {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Fill({:?} -> {:?})", self.order_id, self.completion)
    }
}

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

    fn ask_levels(&self) -> impl Iterator<Item = (&Price, &Level)> {
        self.levels.range(self.best_ask..)
    }

    fn ask_levels_mut(&mut self) -> impl Iterator<Item = (&Price, &mut Level)> {
        self.levels.range_mut(self.best_ask..)
    }

    fn bid_levels(&self) -> impl Iterator<Item = (&Price, &Level)> {
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

    pub fn execute_market_buy(
        &mut self,
        target_vol: Volume,
        fills: &mut Vec<Fill>,
    ) -> MarketTxnOutcome {
        let res = execute_market_txn(self.ask_levels_mut(), target_vol, fills);
        if let MarketTxnOutcome::Success { new_best_price } = res {
            self.best_ask = new_best_price
        };
        res
    }

    pub fn execute_market_sell(
        &mut self,
        target_vol: Volume,
        fills: &mut Vec<Fill>,
    ) -> MarketTxnOutcome {
        let res = execute_market_txn(self.bid_levels_mut(), target_vol, fills);
        if let MarketTxnOutcome::Success { new_best_price } = res {
            self.best_bid = new_best_price
        };
        res
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
                return Cancellation::WasCancelled;
            }
        }
        Cancellation::NotFound
    }
}

enum Cancellation {
    WasCancelled,
    NotFound,
}

#[derive(PartialEq, Eq, Debug)]
pub enum MarketTxnOutcome {
    Success { new_best_price: Price },
    InsufficientVolume { volume_transacted: Volume },
}

impl MarketTxnOutcome {
    pub fn result(self) -> Result<Price, Volume> {
        match self {
            MarketTxnOutcome::Success { new_best_price } => Ok(new_best_price),
            MarketTxnOutcome::InsufficientVolume { volume_transacted } => Err(volume_transacted),
        }
    }
}

fn execute_market_txn<'a>(
    price_levels: impl Iterator<Item = (&'a Price, &'a mut Level)>,
    target_vol: Volume,
    fills: &mut Vec<Fill>,
) -> MarketTxnOutcome {
    let mut remaining_txn_vol = target_vol;
    for (price, level) in price_levels {
        if remaining_txn_vol == Volume::new(0) {
            // we're done
            return MarketTxnOutcome::Success {
                new_best_price: *price,
            };
        } else if remaining_txn_vol >= level.total_volume {
            // will exhaust this level
            remaining_txn_vol -= level.total_volume;
            for q in level.iter_quotes() {
                fills.push(Fill::new(q.order_id, FillCompletion::Full));
            }
            level.clear();
            // continue to next price level
        } else {
            // will end at this level
            level.total_volume -= remaining_txn_vol;

            let mut tombstone_inc = 0;
            for q in level.iter_quotes_mut() {
                if remaining_txn_vol < q.volume {
                    // partial fill (and we're done)
                    q.volume -= remaining_txn_vol;
                    fills.push(Fill::new(
                        q.order_id,
                        FillCompletion::Partial(remaining_txn_vol),
                    ));
                    break;
                } else if remaining_txn_vol == q.volume {
                    fills.push(Fill::new(q.order_id, FillCompletion::Full));
                    *q = Quote::tombstone();
                    break;
                } else {
                    // complete fill (and continue)
                    remaining_txn_vol -= q.volume;
                    fills.push(Fill::new(q.order_id, FillCompletion::Full));
                    *q = Quote::tombstone();
                    tombstone_inc += 1;
                }
            }
            level.tombstone_count += tombstone_inc;
            level.maybe_compact();
            // we're done
            return MarketTxnOutcome::Success {
                new_best_price: *price,
            };
        }
    }
    // if we get here then we used up all the volume
    MarketTxnOutcome::InsufficientVolume {
        volume_transacted: target_vol - remaining_txn_vol,
    }
}

pub enum Outcome {
    PlacedExisting,
    PlacedNew,
    PlacedNewBest,
    CrossedSpread,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FillCompletion {
    Full,
    Partial(Volume),
}

pub enum OrderType {
    MarketBuy {
        volume: Volume,
        bid_balance: Balance,
    },
    MarketSell {
        volume: Volume,
        ask_balance: Balance,
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
}

pub struct Order {
    id: OrderId,
    typ: OrderType,
}

pub fn run_orderbook_event_loop(rx_order: Receiver<Order>, tx_fill: Sender<Fill>) {
    let mut book = OrderBook::new();
    let mut fills_buffer = Vec::with_capacity(1000);
    for order in rx_order {
        match order.typ {
            OrderType::MarketBuy {
                volume,
                bid_balance,
            } => {
                let fill = match book.execute_market_buy(volume, &mut fills_buffer) {
                    MarketTxnOutcome::Success { .. } => Fill::new(order.id, FillCompletion::Full),
                    MarketTxnOutcome::InsufficientVolume { volume_transacted } => {
                        Fill::new(order.id, FillCompletion::Partial(volume_transacted))
                    }
                };
                fills_buffer.push(fill)
            }
            OrderType::MarketSell {
                volume,
                ask_balance,
            } => {
                let fill = match book.execute_market_sell(volume, &mut fills_buffer) {
                    MarketTxnOutcome::Success { .. } => Fill::new(order.id, FillCompletion::Full),
                    MarketTxnOutcome::InsufficientVolume { volume_transacted } => {
                        Fill::new(order.id, FillCompletion::Partial(volume_transacted))
                    }
                };
                fills_buffer.push(fill)
            }
            OrderType::LimitBuy { .. } => todo!(),
            OrderType::LimitSell { .. } => todo!(),
            OrderType::Cancel { price, order_id } => {
                match book.cancel(price, order_id) {
                    Cancellation::WasCancelled => todo!(),
                    Cancellation::NotFound => todo!(),
                };
                continue;
            }
        }
        for &fill in fills_buffer.iter() {
            tx_fill.send(fill).expect("tx_fill send failed");
        }
        fills_buffer.clear();
    }
}

#[cfg(test)]
mod tests {
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
    fn f(i: u64) -> Fill {
        Fill::new(o(i), FillCompletion::Full)
    }
    fn fp(i: u64, p: u64) -> Fill {
        Fill::new(o(i), FillCompletion::Partial(v(p)))
    }
    fn q(q: u64, v: u64) -> Quote {
        Quote {
            order_id: OrderId::new(q),
            volume: Volume::new(v),
        }
    }
    fn lb(id: u64, price: u64, vol: u64) -> Order {
        Order {
            id: o(id),
            typ: OrderType::LimitBuy {
                price: p(price),
                volume: v(vol),
            },
        }
    }
    fn mb(id: u64, vol: u64) -> Order {
        Order {
            id: o(id),
            typ: OrderType::MarketBuy {
                volume: v(vol),
                bid_balance: b(10000),
            },
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
            let mut fills = Vec::new();
            ob.execute_market_buy(v(1), &mut fills)
                .result()
                .expect("buy failed");
            let expect_fills = &[fp(5, 1)];
            assert_eq!(fills, expect_fills);
            assert_eq!(ob.best_ask(), p(35));
        }
        {
            let mut fills = Vec::new();
            ob.execute_market_buy(v(24), &mut fills)
                .result()
                .expect("buy failed");
            let expect_fills = &[f(5), fp(6, 15)];
            assert_eq!(fills, expect_fills);
            assert_eq!(ob.best_ask(), p(40));
        }
        {
            let mut fills = Vec::new();
            ob.execute_market_buy(v(5), &mut fills)
                .result()
                .expect("buy failed");
            let expect_fills = &[f(6)];
            assert_eq!(fills, expect_fills);
            assert_eq!(ob.best_ask(), p(45));
        }
        {
            let mut fills = Vec::new();
            let filled_vol = ob
                .execute_market_buy(v(500), &mut fills)
                .result()
                .unwrap_err();
            assert_eq!(filled_vol, v(70));
            let expect_fills = &[f(7), f(8)];
            assert_eq!(fills, expect_fills);
            assert_eq!(ob.best_ask(), p(45));
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
            let res = book.execute_market_buy(v(11), &mut fills);
            assert_eq!(
                res,
                MarketTxnOutcome::Success {
                    new_best_price: p(10)
                }
            );
            let expect_fills = &[f(1), fp(2, 1)];
            assert_eq!(fills, expect_fills);
            assert_eq!(book.ask_volume(), v(19));
        }
        {
            let mut fills = Vec::new();
            let res = book.execute_market_buy(v(9), &mut fills);
            assert_eq!(
                res,
                MarketTxnOutcome::Success {
                    new_best_price: p(10)
                }
            );
            let expect_fills = &[f(2)];
            assert_eq!(fills, expect_fills);
        }
    }

    #[test]
    fn test_execute_market_sell_simple() {
        let mut ob = quick_book();
        {
            let mut fills = Vec::new();
            let res = ob.execute_market_sell(v(90), &mut fills);
            assert_eq!(res.result().unwrap(), p(10));
            assert_eq!(fills, &[f(4), f(3), f(2), fp(1, 30)])
        }
        {
            let mut fills = Vec::new();
            let res = ob.execute_market_sell(v(22), &mut fills);
            assert_eq!(res.result().unwrap_err(), v(10));
            assert_eq!(fills, &[f(1)])
        }
    }

    #[test]
    fn test_zero_volume_scenarios() {
        let mut book = OrderBook::new();
        let mut fills = Vec::new();
        {
            // TODO a zero-volume order should probably return success?
            book.execute_market_buy(v(0), &mut fills)
                .result()
                .unwrap_err();
            book.execute_market_sell(v(0), &mut fills)
                .result()
                .unwrap_err();
        }
        {
            book.execute_market_buy(v(10), &mut fills)
                .result()
                .unwrap_err();
            book.execute_market_sell(v(10), &mut fills)
                .result()
                .unwrap_err();
        }
        {
            book.add_ask(p(20), q(1, 10));
            book.add_bid(p(10), q(1, 10));
            book.execute_market_buy(v(0), &mut fills).result().unwrap();
            book.execute_market_sell(v(0), &mut fills).result().unwrap();
        }
    }

    #[test]
    fn test_run_order_book() {
        let (tx_order, rx_order) = crossbeam_channel::bounded(1000);
        let (tx_fill, rx_fill) = crossbeam_channel::bounded(1000);
        std::thread::spawn(move || run_orderbook_event_loop(rx_order, tx_fill));

        tx_order.send(lb(1, 10, 30)).unwrap();
        tx_order.send(lb(2, 10, 20)).unwrap();
        tx_order.send(lb(3, 10, 10)).unwrap();
        tx_order.send(mb(4, 20)).unwrap();

        let f1 = rx_fill.try_recv().unwrap();
        let f2 = rx_fill.try_recv().unwrap();
        assert_eq!(f1, f(1));
        assert_eq!(f2, fp(2, 10));
        assert!(rx_fill.try_recv().is_err());
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
        book.execute_market_buy(v(1), &mut fills).result().unwrap();
        assert_eq!(fills, &[fp(6, 1)]);
    }
}
