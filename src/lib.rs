//! A fixed-capacity, lock-free single-producer, single-consumer ring buffer.
//!
//! `tiny-ring` exists for the case where you know the maximum burst size up
//! front and want to move values between exactly two threads without a mutex:
//! audio callbacks, log shippers, packet capture, simple work handoff. The
//! buffer is allocated once, never grows, and never blocks.
//!
//! # Invariants
//!
//! * Exactly one thread calls [`Ring::push`] and exactly one thread calls
//!   [`Ring::pop`]. Neither method takes `&mut self`; the split is enforced by
//!   convention and checked by the type system only insofar as `Ring<T>` is
//!   `Sync` when `T: Send`.
//! * Capacity is rounded up to a power of two, so the slot index is a mask.
//! * Indices are free-running `usize` counters. The distance between them is
//!   the fill level; comparisons use `wrapping_sub`, so a counter wrap is
//!   harmless.
//! * A slot is written by the producer before the head index is published with
//!   `Release`; the consumer reads the head with `Acquire` before touching the
//!   slot, then releases the tail slot after moving out.
//!
//! # Failure modes
//!
//! * Push into a full buffer returns [`PushError`] carrying the value back, so
//!   nothing is dropped silently. Backpressure policy belongs to the caller.
//! * Pop from an empty buffer returns [`PopError`].
//! * `with_capacity(0)` panics; there is no meaningful zero-capacity buffer.
//! * Panics inside a value's `Drop` while it sits in the buffer are the
//!   caller's problem: `Ring` does not implement `Drop` yet, so items still in
//!   the buffer when it is dropped are leaked. Use [`Ring::pop`] until empty
//!   if `T` owns resources.
//!
//! # Example
//!
//! ```
//! use tiny_ring::Ring;
//!
//! let ring = Ring::with_capacity(4);
//! ring.push(7).unwrap();
//! assert_eq!(ring.pop(), Ok(7));
//! assert_eq!(ring.pop(), Err(tiny_ring::PopError));
//!
//! let mut batch = Vec::new();
//! assert_eq!(ring.drain_into(&mut batch, 4), 0);
//! ```
#![forbid(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs)]

mod error;
mod ops;
mod ring;

pub use error::{PopError, PushError};
pub use ring::Ring;

/// Rounds `requested` up to the next power of two, minimum 1.
///
/// Callers use this to discover the capacity they will actually get; it is the
/// same rounding [`Ring::with_capacity`] applies.
///
/// ```
/// assert_eq!(tiny_ring::capacity_for(5), 8);
/// ```
pub const fn capacity_for(requested: usize) -> usize {
    let mut capacity = 1usize;
    while capacity < requested {
        capacity <<= 1;
    }
    capacity
}
