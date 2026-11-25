# tiny-ring

A fixed-capacity, lock-free ring buffer for exactly one producer and one
consumer. No mutex, no allocation after construction, no growth, no blocking.

## Why

Producer/consumer handoff on a hot path — an audio callback feeding a writer
thread, a capture loop feeding a parser, a sensor task feeding a logger — is
usually easier to reason about with a bounded buffer than with an unbounded
channel. When the bound is real, a closed queue is the design, so the buffer is
allocated once at a power-of-two size and indices are free-running counters
advanced with `wrapping_add`.

`std::sync::mpsc` allocates and grows; `tiny-ring` fits in a few lines of
reasoning about two atomics and one mask.

## Install

```toml
[dependencies]
tiny-ring = "0.1"
```

## Usage

```rust
use tiny_ring::Ring;

let ring = Ring::with_capacity(64); // rounds up to 64
ring.push(1u8).unwrap();
assert_eq!(ring.pop(), Ok(1));
```

Full queue semantics: `push` returns the value inside `PushError`, `pop` on an
empty queue returns `PopError`. Both are cheap and allocation-free, so the
backpressure policy stays with the caller:

```rust
use tiny_ring::Ring;

fn drain(ring: &Ring<u8>, out: &mut Vec<u8>) {
    while let Ok(value) = ring.pop() {
        out.push(value);
    }
}
```

Wrap the buffer in an `Arc` and move the clone into the producer thread; the
buffer is `Sync` when `T: Send`. Only one thread may call `push` and only one
may call `pop`.

## Invariants

* Capacity is rounded up to a power of two so slot lookup is a mask.
* Indices are `usize` counters compared with `wrapping_sub`; a wrap is harmless.
* The producer writes a slot before publishing `head` with `Release`; the
  consumer reads `head` with `Acquire` before touching that slot and releases
  `tail` after moving the value out.
* A slot is only ever reachable by one side at a time.

## Failure modes

* `with_capacity(0)` panics.
* Items still buffered when the `Ring` is dropped are leaked; drain first if
  `T` owns resources.
* The buffer is SPSC, not MPMC. A second producer silently corrupts the fill
  level.
* `Ring` does not implement `Drop`, so `mem::forget` of the buffer is free.

## Layout

```
src/lib.rs      crate docs, re-exports, capacity rounding
src/ring.rs     the buffer: slots, indices, push/pop
src/error.rs    PushError (carries the value), PopError
benches/        single-threaded round-trip probe
```

## Benchmarks

`cargo bench` runs `benches/push_pop.rs`, which times push/pop round trips on a
1024-slot buffer. A release build did 10M round trips in 11.3 ms (about 1.1 ns
each) on the development machine; the buffer never left cache. The absolute
number is less interesting than the shape staying flat across releases.

## Testing

`cargo test` covers capacity rounding, full/empty edges, 10k index wraps, and a
two-thread 100k-item ordering check.

## License

MIT. See [LICENSE](LICENSE).
