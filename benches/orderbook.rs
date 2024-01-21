use cambiare::{Balance, OrderBook, OrderId, Price, Quote, Volume};
use criterion::{criterion_group, criterion_main, Criterion};

pub fn orderbook_benchmark(c: &mut Criterion) {
    c.bench_function("add ask then execute market buy", |b| {
        let mut book = OrderBook::new();
        let mut fills = Vec::new();
        b.iter(|| {
            book.add_ask(
                Price::new(500),
                Quote::new(OrderId::new(1), Volume::new(50)),
            );
            let _ = book
                .execute_market_buy(
                    OrderId::new(2),
                    Volume::new(500),
                    Balance::new(1000000),
                    &mut fills,
                )
                .exhausted();
            fills.clear()
        });
    });

    c.bench_function("add bid then execute market sell", |b| {
        let mut book = OrderBook::new();
        let mut fills = Vec::new();
        b.iter(|| {
            book.add_bid(
                Price::new(500),
                Quote::new(OrderId::new(1), Volume::new(50)),
            );
            let _ = book
                .execute_market_sell(OrderId::new(2), Volume::new(500), &mut fills)
                .exhausted();
            fills.clear()
        });
    });

    c.bench_function("add 500 volume then execute market buy", |b| {
        let mut book = OrderBook::new();
        book.add_ask(
            Price::new(500),
            Quote::new(OrderId::new(1), Volume::new(50)),
        );
        let mut fills = Vec::new();
        b.iter(|| {
            // println!("ITER");
            // assert_eq!(book.ask_volume(), Volume::new(50));
            // add 500 volume, sell 500 volume (leaving 'original' 50 volume)
            // this will ensure tombstones are generated
            for (order_id, (price, vol)) in [
                (100, 100),
                (200, 100),
                (400, 50),
                (300, 50),
                (400, 50),
                (500, 50),
                (500, 50),
                (100, 50),
            ]
            .into_iter()
            .enumerate()
            {
                book.add_ask(
                    Price::new(price),
                    Quote::new(OrderId::new((order_id + 1) as u64), Volume::new(vol)),
                );
            }

            let _ = book
                .execute_market_buy(
                    OrderId::new(100),
                    Volume::new(500),
                    Balance::new(1000000),
                    &mut fills,
                )
                .filled();
            // assert_eq!(fills.len(), 8);
            // assert_eq!(book.ask_volume(), Volume::new(50));
            fills.clear();
        })
    });
}

criterion_group!(benches, orderbook_benchmark);
criterion_main!(benches);
