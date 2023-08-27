use std::collections::{btree_map::Entry, BTreeMap};

use crossbeam_channel::{Receiver, Sender};

// Order arrives --> Check user balance --> Enter into book --> return result to user

macro_rules! newtype {
    ($newtype: ident) => {
        #[derive(
            PartialEq,
            Eq,
            Hash,
            Debug,
            Clone,
            Copy,
            Default,
            PartialOrd,
            Ord,
            derive_more::Add,
            derive_more::AddAssign,
            derive_more::Sub,
            derive_more::SubAssign,
            derive_more::Constructor,
        )]
        pub struct $newtype(u64);
    };
}

newtype!(Price);
newtype!(Volume);
newtype!(UserId);
newtype!(OrderId);

pub struct OrderBook {
    best_ask: Price,
    best_bid: Price,
    levels: BTreeMap<Price, (Volume, Vec<Quote>)>,
}

#[derive(derive_more::Constructor)]
pub struct Quote {
    order_id: OrderId,
    volume: Volume,
}
impl Quote {
    fn tombstone() -> Quote {
        Quote {
            order_id: OrderId(u64::MAX),
            volume: Volume(u64::MAX),
        }
    }

    fn is_tombstone(&self) -> bool {
        self.order_id.0 == u64::MAX
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, derive_more::Constructor)]
pub struct Fill {
    order_id: OrderId,
    completion: FillCompletion,
}

impl OrderBook {
    pub fn new() -> Self {
        OrderBook {
            best_ask: Price(u64::MAX),
            best_bid: Price(u64::MIN),
            levels: BTreeMap::new(),
        }
    }

    pub fn best_bid(&self) -> Price {
        self.best_bid
    }

    pub fn best_ask(&self) -> Price {
        self.best_ask
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
                v.insert((quote.volume, vec![quote]));
            }
            // existing level
            Entry::Occupied(mut o) => {
                let (vol, quotes) = o.get_mut();
                did_update = *vol != Volume(0);
                *vol += quote.volume;
                quotes.push(quote);
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
                v.insert((quote.volume, vec![quote]));
            }
            Entry::Occupied(mut o) => {
                let (vol, quotes) = o.get_mut();
                did_update = *vol != Volume(0);
                *vol += quote.volume;
                quotes.push(quote);
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
    ) -> Result<(), Volume> {
        let mut remaining_buy_vol = target_vol;
        for (price, (level_vol, quotes)) in self.levels.range_mut(self.best_ask..) {
            if remaining_buy_vol == Volume(0) {
                // we're done
                self.best_ask = *price;
                return Ok(());
            } else if remaining_buy_vol >= *level_vol {
                // will exhaust this level
                remaining_buy_vol -= *level_vol;
                *level_vol = Volume(0);
                for q in quotes.iter() {
                    if q.is_tombstone() {
                        // ignore
                        continue;
                    }
                    fills.push(Fill::new(q.order_id, FillCompletion::Full));
                }
                quotes.clear();
                // don't return here - continue to next price level
                // so we can set the best price correctly
            } else {
                // will end at this level
                *level_vol -= remaining_buy_vol;
                self.best_ask = *price;

                for q in quotes {
                    if q.is_tombstone() {
                        //tombstone, ignore
                        continue;
                    }
                    if remaining_buy_vol < q.volume {
                        // partial fill (and we're done)
                        q.volume -= remaining_buy_vol;
                        fills.push(Fill::new(
                            q.order_id,
                            FillCompletion::Partial(remaining_buy_vol),
                        ));
                        break;
                    } else {
                        // complete fill (and continue)
                        fills.push(Fill::new(q.order_id, FillCompletion::Full));
                        *q = Quote::tombstone();
                    }
                }
                return Ok(());
            }
        }
        // if we get here then we used up all the volume
        Err(target_vol - remaining_buy_vol)
    }

    pub fn execute_market_sell(&mut self, mut sell_vol: Volume) -> Result<(), Volume> {
        todo!()
        // for (price, level_vol) in self.levels.range_mut(..=self.best_bid).rev() {
        //     if sell_vol == Volume(0) {
        //         // we're done
        //         self.best_bid = *price;
        //         return Ok(());
        //     } else if sell_vol >= *level_vol {
        //         // will exhause this level
        //         sell_vol -= *level_vol;
        //         *level_vol = Volume(0);
        //     } else {
        //         *level_vol -= sell_vol;
        //         self.best_bid = *price;
        //         return Ok(());
        //     }
        // }
        // // we used up all the volume !?
        // Err(sell_vol)
    }

    // pub fn execute_limit_buy(
    //     &mut self,
    //     limit_price: Price,
    //     mut buy_vol: Volume,
    // ) -> Result<(), Volume> {
    //     if self.best_ask > limit_price {
    //         todo!()
    //     }
    //     for (price, level_vol) in self.levels.range_mut(self.best_ask..) {
    //         if buy_vol == Volume(0) {
    //             // we're done
    //             self.best_ask = *price;
    //             return Ok(());
    //         } else if buy_vol >= *level_vol {
    //             // will exhaust this level
    //             buy_vol -= *level_vol;
    //             *level_vol = Volume(0);
    //         } else {
    //             // will end at this level
    //             *level_vol -= buy_vol;
    //             self.best_ask = *price;
    //             return Ok(());
    //         }
    //     }
    //     // we used up all the volume !?
    //     Err(buy_vol)
    // }

    pub fn spread(&self) -> Price {
        self.best_ask - self.best_bid
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

enum Order {
    MarketBuy { volume: Volume },
    MarketSell { volume: Volume },
    LimitBuy { price: Price, volume: Volume },
    LimitSell { price: Price, volume: Volume },
}

fn listen(rx_order: Receiver<Order>, tx_fill: Sender<Fill>) -> Result<(), ()> {
    let mut book = OrderBook::new();
    let mut fills_buffer = Vec::with_capacity(1000);
    for ev in rx_order {
        match ev {
            Order::MarketBuy { volume } => {
                book.execute_market_buy(volume, &mut fills_buffer);
                for &fill in fills_buffer.iter() {
                    tx_fill.send(fill);
                }
                fills_buffer.clear();
            }
            Order::MarketSell { volume } => {
                todo!()
                // book.execute_market_sell(volume, &mut fills_buffer);
            }
            Order::LimitBuy { price, volume } => {}
            Order::LimitSell { price, volume } => todo!(),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn p(v: u64) -> Price {
        Price(v)
    }
    fn v(v: u64) -> Volume {
        Volume(v)
    }
    fn o(v: u64) -> OrderId {
        OrderId(v)
    }
    fn q(q: u64, v: u64) -> Quote {
        Quote {
            order_id: OrderId(q),
            volume: Volume(v),
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
        use FillCompletion as FC;
        let mut ob = quick_book();

        assert_eq!(ob.spread(), p(10));

        {
            let mut fills = Vec::new();
            ob.execute_market_buy(v(1), &mut fills).expect("buy failed");
            let expect_fills = &[Fill::new(o(5), FC::Partial(v(1)))];
            assert_eq!(fills, expect_fills);
            assert_eq!(ob.best_ask(), p(35));
        }
        {
            let mut fills = Vec::new();
            ob.execute_market_buy(v(24), &mut fills)
                .expect("buy failed");
            let expect_fills = &[
                Fill::new(o(5), FC::Full),
                Fill::new(o(6), FC::Partial(v(15))),
            ];
            assert_eq!(fills, expect_fills);
            assert_eq!(ob.best_ask(), p(40));
        }
        {
            let mut fills = Vec::new();
            ob.execute_market_buy(v(5), &mut fills).expect("buy failed");
            let expect_fills = &[Fill::new(o(6), FC::Full)];
            assert_eq!(fills, expect_fills);
            assert_eq!(ob.best_ask(), p(45));
        }
        {
            let mut fills = Vec::new();
            let filled_vol = ob.execute_market_buy(v(500), &mut fills).unwrap_err();
            assert_eq!(filled_vol, v(70));
            let expect_fills = &[Fill::new(o(7), FC::Full), Fill::new(o(8), FC::Full)];
            assert_eq!(fills, expect_fills);
            assert_eq!(ob.best_ask(), p(45));
        }
    }
}
