//! Pump 1..=LIMIT through the buffer and consume it in fixed-size batches,
//! checking that nothing is reordered or lost.
//!
//! Run with `cargo run --example batch_pump --release`.

use std::sync::Arc;

use tiny_ring::Ring;

const LIMIT: u64 = 500_000;
const BATCH: usize = 64;

fn main() {
    let ring = Arc::new(Ring::with_capacity(256));

    let producer = {
        let ring = Arc::clone(&ring);
        std::thread::spawn(move || {
            for value in 1..=LIMIT {
                let mut pending = value;
                loop {
                    match ring.push(pending) {
                        Ok(()) => break,
                        Err(rejected) => {
                            pending = rejected.into_inner();
                            std::thread::yield_now();
                        }
                    }
                }
            }
        })
    };

    let mut batch = Vec::with_capacity(BATCH);
    let mut last = 0u64;
    let mut sum = 0u64;
    let mut seen = 0u64;

    while seen < LIMIT {
        batch.clear();
        let moved = ring.drain_into(&mut batch, BATCH);
        if moved == 0 {
            std::thread::yield_now();
            continue;
        }

        for &value in &batch {
            assert_eq!(value, last + 1, "out of order at {value}");
            last = value;
            sum += value;
        }
        seen += moved as u64;
    }

    producer.join().unwrap();

    let expected = LIMIT * (LIMIT + 1) / 2;
    assert_eq!(sum, expected);
    assert!(ring.is_empty());
    println!("drained {seen} values, checksum {sum}");
}
