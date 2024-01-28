use std::{collections::BTreeMap, sync::Arc};

use crate::order_book;
use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use crossbeam_channel::{Receiver, Sender};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{Price, Volume};

fn app(state: AppState) -> Router {
    Router::new()
        .route("/markets", get(get_markets))
        .route("/market/:symbol/orderbook", get(get_market_orderbook))
        .with_state(Arc::new(state))
}

fn init_markets() -> BTreeMap<TradingPair, MarketState> {
    use Currency::*;
    [TradingPair::new(USD, GBP)]
        .into_iter()
        .map(|t| (t, start_market_in_thread()))
        .collect()
}

pub async fn serve() {
    let app = app(AppState {
        markets: init_markets(),
    });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

struct AppState {
    markets: BTreeMap<TradingPair, MarketState>,
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

struct MarketState {
    volume_24h: f64,
    latest_snapshot: MarketSnapshot,
    order_tx: Sender<order_book::Order>,
    snapshot_rx: Receiver<order_book::OrderBook>,
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
        latest_snapshot: MarketSnapshot {
            book: order_book::OrderBook::default(),
        },
        order_tx,
        snapshot_rx,
    }
}

async fn get_markets(state: State<Arc<AppState>>) -> Json<Vec<TradingPair>> {
    Json(state.markets.keys().copied().collect())
}

#[derive(Serialize, Deserialize, Clone, Debug)]
// Only fo de/serialization - do not mix up with types in order_book.rs
struct ApiOrderbook {
    bid: BTreeMap<Decimal, Decimal>,
    ask: BTreeMap<Decimal, Decimal>,
}

impl ApiOrderbook {
    fn from_order_book(book: &order_book::OrderBook) -> Self {
        todo!()
    }
}

async fn get_market_orderbook(
    state: State<Arc<AppState>>,
    path: Path<String>,
) -> Json<ApiOrderbook> {
    let pair: TradingPair = path.as_str().parse().unwrap();
    let market = state.markets.get(&pair).unwrap();
    let book = ApiOrderbook::from_order_book(&market.latest_snapshot.book);
    Json(book)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum_test::TestServer;
    use Currency::*;

    #[tokio::test]
    async fn test_get_markets() {
        let app = app(AppState {
            markets: init_markets(),
        });
        let server = TestServer::new(app).unwrap();
        let pairs: Vec<TradingPair> = server.get("/markets").await.json();
        assert_eq!(pairs, vec![TradingPair::new(USD, GBP)]);
    }
}
