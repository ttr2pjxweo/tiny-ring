//! Throughput probe for the push/pop fast path.
//!
//! Run with `cargo bench`. Numbers are single-threaded round trips; the point
//! is to catch order-of-magnitude regressions, not to win a leaderboard.

use std::hint::black_box;
use std::time::Instant;

use tiny_ring::Ring;

const ITEMS: u32 = 10_000_000;

fn main() {
    let ring = Ring::with_capacity(1024);
    let start = Instant::now();

    for value in 0..ITEMS {
        ring.push(black_box(value)).unwrap();
        black_box(ring.pop().unwrap());
    }

    let elapsed = start.elapsed();
    let ns_per_round_trip = elapsed.as_nanos() as f64 / f64::from(ITEMS);
    println!(
        "{ITEMS} round trips in {:?} ({ns_per_round_trip:.1} ns/round trip)",
        elapsed
    );
}
