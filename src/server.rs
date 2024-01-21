use std::{collections::BTreeMap, sync::Arc};

use axum::{extract::State, routing::get, Json, Router};
use serde::{Deserialize, Serialize};

fn app(state: AppState) -> Router {
    Router::new()
        .route("/markets", get(get_markets))
        .with_state(Arc::new(state))
}

pub async fn serve() {
    // launch market

    let app = app(AppState {
        markets: BTreeMap::new(),
    });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

struct AppState {
    markets: BTreeMap<TradingPair, MarketState>,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
enum Currency {
    USD,
    GBP,
    EUR,
    JPY,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
struct TradingPair {
    bid: Currency,
    ask: Currency,
}

struct MarketState {
    volume_24h: f64,
}

#[axum::debug_handler]
async fn get_markets(state: State<Arc<AppState>>) -> Json<Vec<TradingPair>> {
    Json(state.markets.keys().copied().collect())
}
