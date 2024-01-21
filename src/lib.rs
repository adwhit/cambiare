use std::collections::HashMap;

use crossbeam_channel::{Receiver, Sender};

use order_book::{Match, MatchType, Order};

pub use order_book::run_orderbook_event_loop;
pub use order_book::{OrderBook, Quote};

mod order_book;
pub mod server;

mod newtypes {
    macro_rules! newtype {
        ($newtype: ident) => {
            #[derive(
                PartialEq,
                Eq,
                Hash,
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
                derive_more::Display,
            )]
            pub struct $newtype(u64);
            impl std::fmt::Debug for $newtype {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}", self.0)
                }
            }
            impl From<u64> for $newtype {
                fn from(other: u64) -> Self {
                    Self(other)
                }
            }
            impl From<$newtype> for u64 {
                fn from(other: $newtype) -> Self {
                    other.0
                }
            }
            impl $newtype {
                pub fn inner(self) -> u64 {
                    self.0
                }
            }
        };
    }
    newtype!(Price);
    newtype!(Volume);
    newtype!(UserId);
    newtype!(OrderId);
    newtype!(Balance);
}

pub use newtypes::{Balance, OrderId, Price, UserId, Volume};

#[derive(
    PartialEq,
    Eq,
    Hash,
    Clone,
    Copy,
    Default,
    PartialOrd,
    Ord,
    derive_more::Constructor,
    derive_more::Display,
)]
pub struct Currency(&'static str);

struct Symbol {
    base: Currency,
    quote: Currency,
}

#[derive(Default)]
struct Accounts {
    accounts: HashMap<UserId, UserAccount>,
    live_orders: HashMap<OrderId, (UserId, AccountOrder)>,
}

#[derive(Default)]
struct UserAccount {
    live_orders: Vec<OrderId>,
    balances: HashMap<Currency, Balance>,
}

pub struct AccountEvent {
    user_id: UserId,
    event: AccountEventType,
}

pub enum AccountEventType {
    Deposit {
        currency: Currency,
        balance: Balance,
    },
    Withdraw {
        currency: Currency,
        balance: Balance,
    },
    PlaceOrder(AccountOrder),
}

enum AccountOrderType {
    MarketBuy { base_qty: Volume },
    MarketSell { base_qty: Volume },
    MarketBuyQ { quote_qty: Volume },
    MarketSellQ { quote_qty: Volume },
    LimitBuy { volume: Volume, price: Price },
    LimitSell { volume: Volume, price: Price },
}

#[derive(PartialEq)]
enum Side {
    Buy,
    Sell,
}

impl AccountOrderType {
    fn side(&self) -> Side {
        match self {
            AccountOrderType::MarketBuy { .. }
            | AccountOrderType::MarketBuyQ { .. }
            | AccountOrderType::LimitBuy { .. } => Side::Buy,
            AccountOrderType::MarketSell { .. }
            | AccountOrderType::MarketSellQ { .. }
            | AccountOrderType::LimitSell { .. } => Side::Sell,
        }
    }
}

pub struct AccountOrder {
    id: OrderId,
    symbol: Symbol,
    typ: AccountOrderType,
}

pub fn run_account_event_loop(
    rx_acct_event: Receiver<AccountEvent>,
    rx_matches: Receiver<Match>,
    tx_order: Sender<Order>,
    tx_outcome: Sender<String>,
) {
    let mut accounts = Accounts::default();
    crossbeam_channel::select! {
        recv(rx_acct_event) -> msg => {
            handle_account_event(msg.unwrap(), &mut accounts, &tx_order, &tx_outcome)
        }
        recv(rx_matches) -> msg => {
            handle_matched_trade(msg.unwrap(), &mut accounts)
        }
    }
}

fn handle_matched_trade(match_ev: Match, accounts: &mut Accounts) {
    let Some((maker_user_id, maker_acct_order)) =
        accounts.live_orders.get(&match_ev.maker_order_id)
    else {
        todo!()
    };
    let Some((taker_user_id, taker_acct_order)) =
        accounts.live_orders.get(&match_ev.taker_order_id)
    else {
        todo!()
    };
    let Some(maker_acct) = accounts.accounts.get_mut(&maker_user_id) else {
        todo!()
    };
    let Some(taker_acct) = accounts.accounts.get_mut(&taker_user_id) else {
        todo!()
    };

    let maker_side = maker_acct_order.typ.side();
    let taker_side = taker_acct_order.typ.side();
    assert!(maker_side != taker_side);
    match match_ev.typ {
        MatchType::MakerFilled => {
            todo!()
        }
        MatchType::TakerFilled => todo!(),
        MatchType::BothFilled => todo!(),
    }
}

fn handle_account_event(
    ev: AccountEvent,
    accounts: &mut Accounts,
    tx_order: &Sender<Order>,
    tx_outcome: &Sender<String>,
) {
    match ev.event {
        AccountEventType::Deposit { currency, balance } => {
            let entry = accounts.accounts.entry(ev.user_id).or_default();
            let bal = entry.balances.entry(currency).or_default();
            *bal += balance;
        }
        AccountEventType::Withdraw { currency, balance } => {
            let Some(acct) = accounts.accounts.get_mut(&ev.user_id) else {
                tx_outcome.send("insufficient balance".into()).unwrap();
                return;
            };
            let Some(bal) = acct.balances.get_mut(&currency) else {
                tx_outcome.send("insufficient balance".into()).unwrap();
                return;
            };
            if *bal < balance {
                tx_outcome.send("insufficient balance".into()).unwrap();
                return;
            }
            *bal += balance;
            tx_outcome.send("balance withdrawn".into()).unwrap();
        }
        AccountEventType::PlaceOrder(acct_order) => match acct_order.typ {
            AccountOrderType::MarketBuy { base_qty } => {
                let Some(acct) = accounts.accounts.get_mut(&ev.user_id) else {
                    tx_outcome.send("insufficient balance".into()).unwrap();
                    return;
                };
                let Some(quote_bal) = acct.balances.get_mut(&acct_order.symbol.quote) else {
                    tx_outcome.send("insufficient balance".into()).unwrap();
                    return;
                };
                todo!()
            }
            AccountOrderType::MarketSell { base_qty } => {
                let Some(acct) = accounts.accounts.get_mut(&ev.user_id) else {
                    tx_outcome.send("insufficient balance".into()).unwrap();
                    return;
                };
                let Some(base_bal) = acct.balances.get_mut(&acct_order.symbol.base) else {
                    tx_outcome.send("insufficient balance".into()).unwrap();
                    return;
                };
                // this is the easiest order type - just check we have enough of
                // the thing we want to sell
                if base_bal.inner() < base_qty.into() {
                    tx_outcome.send("insufficient balance".into()).unwrap();
                    return;
                }
                *base_bal -= Balance::from(base_qty.inner());
                acct.live_orders.push(acct_order.id);
                let order = Order {
                    id: acct_order.id,
                    typ: order_book::OrderType::MarketSell { base_qty },
                };
                accounts
                    .live_orders
                    .insert(acct_order.id, (ev.user_id, acct_order));
                tx_order.send(order);
            }
            AccountOrderType::LimitBuy { volume, price } => todo!(),
            AccountOrderType::LimitSell { volume, price } => todo!(),
            AccountOrderType::MarketBuyQ { quote_qty } => {}
            AccountOrderType::MarketSellQ { quote_qty } => todo!(),
        },
    }
}
