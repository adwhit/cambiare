use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use crate::{order_book, OrderId, Price, UserId, Volume};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use crossbeam_channel::{Receiver, Sender};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use serde::{Deserialize, Serialize};

fn start_new_markets(
    symbols: impl Iterator<Item = TradingPair>,
) -> BTreeMap<TradingPair, MarketState> {
    symbols.map(|t| (t, start_market_in_thread())).collect()
}

pub async fn serve() {
    let app = app(AppState::new());
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(
    Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord, derive_more::FromStr,
)]
enum Currency {
    EUR,
    GBP,
    JPY,
    USD,
}

#[derive(
    Serialize,
    Deserialize,
    Clone,
    Copy,
    derive_more::Constructor,
    PartialEq,
    Eq,
    Debug,
    PartialOrd,
    Ord,
)]
struct TradingPair {
    bid: Currency,
    ask: Currency,
}

#[derive(Debug)]
struct BadTradingPairError;

impl std::str::FromStr for TradingPair {
    type Err = BadTradingPairError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((l, r)) = s.split_once("_") else {
            return Err(BadTradingPairError);
        };
        let Ok(l) = l.parse() else {
            return Err(BadTradingPairError);
        };
        let Ok(r) = r.parse() else {
            return Err(BadTradingPairError);
        };
        Ok(TradingPair::new(l, r))
    }
}

struct MarketSnapshot {
    book: order_book::OrderBook,
}

struct UserState {
    open_orders: Vec<OrderId>,
    balances: HashMap<Currency, Volume>,
}

#[derive(Default)]
struct UserStates {
    states: HashMap<UserId, UserState>,
}

struct MarketState {
    volume_24h: f64,
    // TODO - better to have a RWLock?
    latest_snapshot: Mutex<MarketSnapshot>,
    order_tx: Sender<order_book::Order>,
    snapshot_rx: Receiver<order_book::OrderBook>,
}

impl MarketState {
    fn update_snapshot(&self) {
        self.order_tx
            .send(order_book::Order {
                id: 0xbeef.into(),
                typ: order_book::OrderType::SendSnapshot,
            })
            .unwrap();
        let snapshot = self.snapshot_rx.recv().unwrap();
        *self.latest_snapshot.lock().unwrap() = MarketSnapshot { book: snapshot };
    }

    fn latest_snapshot(&self) -> ApiOrderbook {
        let guard = self.latest_snapshot.lock().unwrap();
        ApiOrderbook::from_order_book(&guard.book)
    }

    fn place_order(&self, order_type: ApiOrderType) -> Result<OrderId, ()> {
        use order_book::OrderType as O;
        use ApiOrderType as A;
        let order_typ = match order_type {
            A::LimitBuy { price, volume } => O::LimitBuy {
                price: Price::try_from(price).unwrap(),
                volume: Volume::try_from(volume).unwrap(),
            },
            A::LimitSell { price, volume } => todo!(),
            A::MarketBuy { volume } => todo!(),
            A::MarketSell { volume } => O::MarketSell {
                base_qty: volume.try_into().unwrap(),
            },
        };
        // TODO lock user balance and get order_id
        let order_id = 123.into();

        let order = order_book::Order {
            id: order_id,
            typ: order_typ,
        };
        self.order_tx.send(order).unwrap();
        Ok(order_id)
    }
}

fn start_market_in_thread() -> MarketState {
    let (order_tx, order_rx) = crossbeam_channel::unbounded();
    let (match_tx, match_rx) = crossbeam_channel::unbounded();
    let (snapshot_tx, snapshot_rx) = crossbeam_channel::unbounded();

    std::thread::spawn(move || {
        order_book::run_orderbook_event_loop(order_rx, match_tx, snapshot_tx);
    });

    MarketState {
        volume_24h: 0.0,
        latest_snapshot: Mutex::new(MarketSnapshot {
            book: order_book::OrderBook::default(),
        }),
        order_tx,
        snapshot_rx,
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
// Only fo de/serialization - do not mix up with types in order_book.rs
struct ApiOrderbook {
    bid: BTreeMap<Decimal, Decimal>,
    ask: BTreeMap<Decimal, Decimal>,
}

impl ApiOrderbook {
    fn from_order_book(book: &order_book::OrderBook) -> Self {
        let bid = book
            .bid_levels()
            .map(|(p, l)| {
                (
                    Decimal::from_u64(p.inner()).unwrap(),
                    Decimal::from_u64(l.total_volume().inner()).unwrap(),
                )
            })
            .collect();
        let ask = book
            .ask_levels()
            .map(|(p, l)| {
                (
                    Decimal::from_u64(p.inner()).unwrap(),
                    Decimal::from_u64(l.total_volume().inner()).unwrap(),
                )
            })
            .collect();
        Self { bid, ask }
    }
}

struct AppState {
    users: UserStates,
    markets: BTreeMap<TradingPair, MarketState>,
}

impl AppState {
    fn new() -> Self {
        Self {
            users: UserStates::default(),
            markets: Default::default(),
        }
    }
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/markets", get(get_markets))
        .route("/market/:symbol/orderbook", get(get_market_orderbook))
        .route("/market/:symbol/order", post(place_order))
        .with_state(Arc::new(state))
}

async fn get_markets(state: State<Arc<AppState>>) -> Json<Vec<TradingPair>> {
    Json(state.markets.keys().copied().collect())
}

async fn get_market_orderbook(
    state: State<Arc<AppState>>,
    path: Path<String>,
) -> Result<Json<ApiOrderbook>, StatusCode> {
    let Ok(pair) = path.as_str().parse::<TradingPair>() else {
        return Err(StatusCode::NOT_FOUND);
    };
    let Some(market) = state.markets.get(&pair) else {
        return Err(StatusCode::NOT_FOUND);
    };
    // TODO probably don't need to update market every time
    market.update_snapshot();
    let book = market.latest_snapshot();
    Ok(Json(book))
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum ApiOrderType {
    LimitBuy { price: Decimal, volume: Decimal },
    LimitSell { price: Decimal, volume: Decimal },
    MarketBuy { volume: Decimal },
    MarketSell { volume: Decimal },
}

#[serde_with::serde_as]
#[derive(Serialize, Deserialize)]
struct PlacedOrder {
    #[serde_as(as = "serde_with::FromInto<u64>")]
    // we want to be careful about directly de/serializing the various IDs
    order_id: OrderId,
}

async fn place_order(
    state: State<Arc<AppState>>,
    path: Path<String>,
    Json(order_type): Json<ApiOrderType>,
) -> Result<Json<PlacedOrder>, StatusCode> {
    let Ok(pair) = path.as_str().parse::<TradingPair>() else {
        return Err(StatusCode::NOT_FOUND);
    };
    let Some(market) = state.markets.get(&pair) else {
        return Err(StatusCode::NOT_FOUND);
    };
    let order_id = market.place_order(order_type).unwrap();
    Ok(Json(PlacedOrder { order_id }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum_test::TestServer;
    use Currency::*;

    fn populate_order_book(order_tx: &Sender<order_book::Order>) {
        use order_book::{Order, OrderType};
        let order1 = Order {
            id: 1.into(),
            typ: OrderType::LimitBuy {
                price: 99.into(),
                volume: 10.into(),
            },
        };
        let order2 = Order {
            id: 2.into(),
            typ: OrderType::LimitSell {
                price: 101.into(),
                volume: 10.into(),
            },
        };
        order_tx.send(order1).unwrap();
        order_tx.send(order2).unwrap();
    }

    fn server() -> TestServer {
        let app = app(AppState::new());
        TestServer::new(app).unwrap()
    }

    fn populated_server() -> TestServer {
        let symbols = ["USD_GBP", "USD_EUR"]
            .into_iter()
            .map(|s| s.parse().unwrap());
        let markets = start_new_markets(symbols);
        let market = markets.get(&TradingPair::new(USD, GBP)).unwrap();
        populate_order_book(&market.order_tx);
        let app = app(AppState::new());
        TestServer::new(app).unwrap()
    }

    #[tokio::test]
    async fn test_get_markets() {
        let server = server();
        let pairs: Vec<TradingPair> = server.get("/markets").await.json();
        assert_eq!(pairs, vec![TradingPair::new(USD, GBP)]);
    }

    #[tokio::test]
    async fn test_get_missing_market_404() {
        let server = server();
        let code = server.get("/market/USDXYZ/orderbook").await.status_code();
        assert_eq!(code, StatusCode::NOT_FOUND);
        let code = server.get("/market/USD_GBP/orderbook").await.status_code();
        assert_eq!(code, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_market_orderbook() {
        let server = populated_server();
        let book: ApiOrderbook = server.get("/market/USD_GBP/orderbook").await.json();
        assert_eq!(book.bid.len(), 1);
        assert_eq!(book.ask.len(), 1);
    }

    #[tokio::test]
    async fn test_place_order() {
        let server = server();
        let order = ApiOrderType::LimitBuy {
            price: Decimal::from(100),
            volume: Decimal::from(500),
        };
        let order: PlacedOrder = server
            .post("market/USD_GBP/order")
            .json(&order)
            .await
            .json();
        assert_eq!(order.order_id, 1.into());
    }
}
