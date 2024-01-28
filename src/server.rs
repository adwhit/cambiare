use std::{collections::BTreeMap, sync::Arc};

use axum::{extract::State, routing::get, Json, Router};
use crossbeam_channel::Sender;
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
        .map(|t| (t, MarketState::default()))
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

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
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

struct MarketState {
    volume_24h: f64,
    query_tx: Sender<MarketQuery>,
    query_rx: Sender<MarketQueryResponse>,
}

enum MarketQuery {
    Orderbook,
}

enum MarketQueryResponse {
    Orderbook(Orderbook),
}

async fn get_markets(state: State<Arc<AppState>>) -> Json<Vec<TradingPair>> {
    Json(state.markets.keys().copied().collect())
}

#[derive(Serialize, Deserialize, Clone, Debug)]
// Only fo de/serialization - do not mix up with types in order_book.rs
struct Orderbook {
    bid: BTreeMap<Decimal, Decimal>,
    ask: BTreeMap<Decimal, Decimal>,
}

async fn get_market_orderbook(
    state: State<Arc<AppState>>,
) -> Json<BTreeMap<TradingPair, Orderbook>> {
    todo!()
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
