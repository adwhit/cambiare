use std::collections::HashMap;

use crossbeam_channel::{Receiver, Sender};

use order_book::Order;

pub use order_book::run_orderbook_event_loop;
pub use order_book::{OrderBook, Quote};

mod order_book;

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
    bid: Currency,
    ask: Currency,
}

#[derive(Default)]
struct Accounts {
    accounts: HashMap<UserId, UserAccount>,
    orders: HashMap<OrderId, UserId>,
}

#[derive(Default)]
struct UserAccount {
    orders: Vec<OrderId>,
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
    PlaceOrder(Order),
}

struct AccountOrder {
    user_id: UserId,
    order: Order,
}

pub fn run_account_event_loop(
    rx_acct_event: Receiver<AccountEvent>,
    tx_order: Sender<Order>,
    tx_outcome: Sender<String>,
) {
    let mut accounts = Accounts::default();
    for ev in rx_acct_event {
        match ev.event {
            AccountEventType::Deposit { currency, balance } => {
                let entry = accounts.accounts.entry(ev.user_id).or_default();
                let bal = entry.balances.entry(currency).or_default();
                *bal += balance;
            }
            AccountEventType::Withdraw { currency, balance } => {
                let Some(acct) = accounts.accounts.get_mut(&ev.user_id) else {
                    tx_outcome.send("insufficient balance".into()).unwrap();
                    continue;
                };
                let Some(bal) = acct.balances.get_mut(&currency) else {
                    tx_outcome.send("insufficient balance".into()).unwrap();
                    continue;
                };
                if *bal < balance {
                    tx_outcome.send("insufficient balance".into()).unwrap();
                    continue;
                }
                *bal += balance;
                tx_outcome.send("balance withdrawn".into()).unwrap();
            }
            AccountEventType::PlaceOrder(_) => {
                todo!()
            }
        }
    }
}
