use std::collections::{btree_map::Entry, BTreeMap};

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
pub struct Price(u64);

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
pub struct Volume(u64);

pub struct OrderBook {
    best_ask: Price,
    best_bid: Price,
    levels: BTreeMap<Price, Volume>,
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

    pub fn add_bid(&mut self, price: Price, volume: Volume) -> Outcome {
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

    pub fn add_ask(&mut self, price: Price, volume: Volume) -> Outcome {
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

    pub fn execute_buy_best_price(&mut self, mut buy_vol: Volume) -> Result<(), Volume> {
        for (price, level_vol) in self.levels.range_mut(self.best_ask..) {
            if buy_vol == Volume(0) {
                // we're done
                self.best_ask = *price;
                return Ok(());
            } else if buy_vol >= *level_vol {
                // will exhause this level
                buy_vol -= *level_vol;
                *level_vol = Volume(0);
            } else {
                // will end at this level
                *level_vol -= buy_vol;
                self.best_ask = *price;
                return Ok(());
            }
        }
        // we used up all the volume !?
        Err(buy_vol)
    }

    pub fn execute_sell_best_price(&mut self, mut sell_vol: Volume) -> Result<(), Volume> {
        for (price, level_vol) in self.levels.range_mut(..=self.best_bid).rev() {
            if sell_vol == Volume(0) {
                // we're done
                self.best_bid = *price;
                return Ok(());
            } else if sell_vol >= *level_vol {
                // will exhause this level
                sell_vol -= *level_vol;
                *level_vol = Volume(0);
            } else {
                *level_vol -= sell_vol;
                self.best_bid = *price;
                return Ok(());
            }
        }
        // we used up all the volume !?
        Err(sell_vol)
    }

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

        ob.execute_buy_best_price(v(1)).expect("buy failed");
        assert_eq!(ob.best_ask(), p(13));
        ob.execute_buy_best_price(v(24)).expect("buy failed");
        assert_eq!(ob.best_ask(), p(14));
        ob.execute_buy_best_price(v(5)).expect("buy failed");
        assert_eq!(ob.best_ask(), p(15));

        ob.execute_sell_best_price(v(10)).expect("sell failed");
        assert_eq!(ob.best_bid(), p(11));

        assert_eq!(ob.execute_buy_best_price(v(100)), Err(v(70)));
        assert_eq!(ob.execute_sell_best_price(v(200)), Err(v(150)));
    }
}
