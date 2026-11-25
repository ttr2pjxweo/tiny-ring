//! Batched draining, for consumers that would rather pay one atomic round
//! trip per chunk than one per value.

use std::sync::atomic::Ordering;

use crate::Ring;

impl<T> Ring<T> {
    /// Moves up to `max` buffered values into `out`, oldest first, and returns
    /// how many were moved.
    ///
    /// The consumer observes the published head once, claims the contiguous
    /// run behind it, and advances the tail once at the end. Slots in the run
    /// are owned by the consumer for the duration, so the loop needs no further
    /// synchronisation. `out` keeps whatever it held before; the caller decides
    /// whether to reuse or clear the vector.
    ///
    /// Unlike [`Ring::pop`](crate::Ring::pop), an empty buffer is not an error:
    /// the return value of `0` says everything.
    ///
    /// # Example
    ///
    /// ```
    /// use tiny_ring::Ring;
    ///
    /// let ring = Ring::with_capacity(8);
    /// for value in 0..5 {
    ///     ring.push(value).unwrap();
    /// }
    ///
    /// let mut batch = Vec::new();
    /// assert_eq!(ring.drain_into(&mut batch, 3), 3);
    /// assert_eq!(batch, [0, 1, 2]);
    /// ```
    pub fn drain_into(&self, out: &mut Vec<T>, max: usize) -> usize {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        let available = head.wrapping_sub(tail).min(max);

        for offset in 0..available {
            let index = tail.wrapping_add(offset) & self.mask;
            // Safety: every claimed slot sits behind the acquired head and at
            // or ahead of the consumer-owned tail. The tail is published only
            // after the loop, so the producer cannot reuse these slots yet.
            out.push(unsafe { (*self.slots[index].get()).assume_init_read() });
        }

        self.tail.store(tail.wrapping_add(available), Ordering::Release);
        available
    }
}

#[cfg(test)]
mod tests {
    use crate::Ring;

    #[test]
    fn drains_in_order_and_stops_at_max() {
        let ring = Ring::with_capacity(8);
        for value in 0..6 {
            ring.push(value).unwrap();
        }

        let mut out = Vec::new();
        assert_eq!(ring.drain_into(&mut out, 4), 4);
        assert_eq!(out, [0, 1, 2, 3]);
        assert_eq!(ring.len(), 2);

        out.clear();
        assert_eq!(ring.drain_into(&mut out, 4), 2);
        assert_eq!(out, [4, 5]);
        assert_eq!(ring.drain_into(&mut out, 4), 0);
    }

    #[test]
    fn appends_and_keeps_room_usable() {
        let ring = Ring::with_capacity(4);
        let mut out = vec![99];
        ring.push(1u8).unwrap();

        assert_eq!(ring.drain_into(&mut out, 4), 1);
        assert_eq!(out, [99, 1]);

        for value in 0..4u8 {
            ring.push(value).unwrap();
        }
        assert!(ring.is_full());
    }

    #[test]
    fn survives_index_wrap() {
        let ring = Ring::with_capacity(2);
        let mut out = Vec::new();

        for round in 0..5_000u32 {
            ring.push(round).unwrap();
            assert_eq!(ring.drain_into(&mut out, 1), 1);
            assert_eq!(out.pop(), Some(round));
        }
        assert!(ring.is_empty());
    }
}
