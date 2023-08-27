use cambiare::{OrderBook, OrderId, Price, Quote, Volume};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn orderbook_benchmark(c: &mut Criterion) {
    c.bench_function("add 5 asks and execute market buy", |b| {
        let mut book = OrderBook::new();
        let mut fills = Vec::new();
        b.iter(|| {
            for (order_id, price, vol) in [
                (1, 100, 100),
                (2, 200, 100),
                (3, 300, 100),
                (4, 400, 100),
                (5, 500, 100),
            ]
            .into_iter()
            {
                book.add_ask(
                    Price::new(price),
                    Quote::new(OrderId::new(order_id), Volume::new(vol)),
                );
            }
            book.execute_market_buy(Volume::new(1000), &mut fills);
            fills.clear();
        })
    });
}

criterion_group!(benches, orderbook_benchmark);
criterion_main!(benches);
