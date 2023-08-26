use std::collections::{btree_map::Entry, BTreeMap};

fn main() {
    println!("Hello, world!");
}

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
)]
struct Price(u64);

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
)]
struct Volume(u64);

struct OrderBook {
    best_ask: Price,
    best_bid: Price,
    levels: BTreeMap<Price, Volume>,
}

impl OrderBook {
    fn new() -> Self {
        OrderBook {
            best_ask: Price(u64::MAX),
            best_bid: Price(u64::MIN),
            levels: BTreeMap::new(),
        }
    }
}

enum Outcome {
    PlacedExisting,
    PlacedNew,
    PlacedNewBest,
    CrossedSpread,
}

impl OrderBook {
    fn add_bid(&mut self, price: Price, volume: Volume) -> Outcome {
        if self.best_ask <= price {
            return Outcome::CrossedSpread;
        }
        let did_update;
        match self.levels.entry(price) {
            Entry::Vacant(v) => {
                did_update = false;
                v.insert(volume);
            }
            Entry::Occupied(mut o) => {
                let vol = o.get_mut();
                did_update = *vol != Volume(0);
                *vol += volume;
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

    fn add_ask(&mut self, price: Price, volume: Volume) -> Outcome {
        if self.best_bid >= price {
            return Outcome::CrossedSpread;
        }
        let did_update;
        match self.levels.entry(price) {
            Entry::Vacant(v) => {
                did_update = false;
                v.insert(volume);
            }
            Entry::Occupied(mut o) => {
                let vol = o.get_mut();
                did_update = *vol != Volume(0);
                *vol += volume;
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

    fn execute_buy_best_price(&mut self, mut volume: Volume) -> Result<(), Volume> {
        for (price, vol) in self.levels.range_mut(self.best_ask..) {
            if *vol < volume {
                volume -= *vol;
                *vol = Volume(0);
            } else {
                *vol -= volume;
                self.best_ask = *price;
                return Ok(());
            }
        }
        Err(volume)
    }

    fn execute_sell_best_price(&mut self, mut volume: Volume) -> Result<(), Volume> {
        for (price, vol) in self.levels.range_mut(..=self.best_bid).rev() {
            if *vol < volume {
                volume -= *vol;
                *vol = Volume(0);
            } else {
                *vol -= volume;
                self.best_bid = *price;
                return Ok(());
            }
        }
        Err(volume)
    }

    fn spread(&self) -> Price {
        self.best_ask - self.best_bid
    }
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
    #[test]
    fn test_insert_bids_asks_simple() {
        let mut ob = OrderBook::new();
        assert!(matches!(ob.add_bid(p(100), v(200)), Outcome::PlacedNewBest));
        assert!(matches!(ob.add_bid(p(50), v(100)), Outcome::PlacedNew));
        assert!(matches!(
            ob.add_bid(p(100), v(300)),
            Outcome::PlacedExisting
        ));
        assert!(matches!(ob.add_ask(p(101), v(300)), Outcome::PlacedNewBest));
        assert!(matches!(
            ob.add_ask(p(101), v(300)),
            Outcome::PlacedExisting
        ));
        assert_eq!(ob.spread(), p(1));
        assert!(matches!(ob.add_bid(p(101), v(100)), Outcome::CrossedSpread));
        assert!(matches!(ob.add_ask(p(100), v(100)), Outcome::CrossedSpread));
    }

    #[test]
    fn test_execute_best_price() {
        let mut ob = OrderBook::new();
        ob.add_bid(p(10), v(30));
        ob.add_bid(p(11), v(20));
        ob.add_bid(p(12), v(10));

        ob.add_ask(p(13), v(10));
        ob.add_ask(p(14), v(20));
        ob.add_ask(p(15), v(30));

        assert_eq!(ob.spread(), p(1));

        ob.execute_buy_best_price(v(25)).expect("buy failed");
        assert_eq!(ob.spread(), p(2));
        ob.execute_sell_best_price(v(25)).expect("sell failed");
        assert_eq!(ob.spread(), p(3));

        assert_eq!(ob.execute_buy_best_price(v(100)), Err(v(65)));
        assert_eq!(ob.execute_sell_best_price(v(200)), Err(v(165)));
    }
}
