use core::cell::UnsafeCell;
use core::mem::MaybeUninit;

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::{PopError, PushError};

/// A fixed-capacity, single-producer single-consumer ring buffer.
///
/// The producer thread owns [`Ring::push`], the consumer thread owns
/// [`Ring::pop`]. See the crate documentation for the memory-ordering contract.
pub struct Ring<T> {
    /// Next slot the producer will write. Producer-only, published on push.
    pub(crate) head: AtomicUsize,
    /// Next slot the consumer will read. Consumer-only, published on pop.
    pub(crate) tail: AtomicUsize,
    /// `capacity - 1`, valid because capacity is a power of two.
    pub(crate) mask: usize,
    pub(crate) slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

// Safety: the buffer hands out disjoint slot indices. The producer only
// touches the slot at `head`, the consumer only the slot at `tail`, and the
// release/acquire pairs on the two indices publish every write between them.
unsafe impl<T: Send> Sync for Ring<T> {}
unsafe impl<T: Send> Send for Ring<T> {}

impl<T> Ring<T> {
    /// Creates a buffer with room for `requested` values, rounded up to a
    /// power of two.
    ///
    /// # Panics
    ///
    /// Panics if `requested` is zero.
    pub fn with_capacity(requested: usize) -> Self {
        assert!(requested > 0, "capacity must be non-zero");
        let capacity = crate::capacity_for(requested);

        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(UnsafeCell::new(MaybeUninit::uninit()));
        }

        Self {
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            mask: capacity - 1,
            slots: slots.into_boxed_slice(),
        }
    }

    /// Number of slots, i.e. the rounded-up capacity.
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Number of values currently buffered.
    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head.wrapping_sub(tail)
    }

    /// Returns `true` when no values are buffered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` when every slot is occupied.
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    /// Appends a value.
    ///
    /// # Errors
    ///
    /// Returns [`PushError`] with the value when the buffer is full.
    pub fn push(&self, value: T) -> Result<(), PushError<T>> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);

        if head.wrapping_sub(tail) >= self.capacity() {
            return Err(PushError(value));
        }

        let index = head & self.mask;
        // Safety: the producer exclusively owns slot `head`, which the fill
        // check above proved unoccupied.
        unsafe { (*self.slots[index].get()).write(value) };

        self.head.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Removes the oldest value.
    ///
    /// # Errors
    ///
    /// Returns [`PopError`] when the buffer is empty.
    pub fn pop(&self) -> Result<T, PopError> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        if head == tail {
            return Err(PopError);
        }

        let index = tail & self.mask;
        // Safety: the consumer exclusively owns slot `tail`, and the acquire
        // load of `head` showed it lies behind the published index.
        let value = unsafe { (*self.slots[index].get()).assume_init_read() };

        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_rounds_up() {
        let ring: Ring<u8> = Ring::with_capacity(5);
        assert_eq!(ring.capacity(), 8);
    }

    #[test]
    #[should_panic(expected = "capacity must be non-zero")]
    fn zero_capacity_panics() {
        let _: Ring<u8> = Ring::with_capacity(0);
    }

    #[test]
    fn pushes_until_full_then_returns_the_value() {
        let ring = Ring::with_capacity(2);
        ring.push(1).unwrap();
        ring.push(2).unwrap();
        assert!(ring.is_full());
        assert_eq!(ring.push(3).map_err(PushError::into_inner), Err(3));
    }

    #[test]
    fn survives_index_wrap() {
        let ring = Ring::with_capacity(4);
        for round in 0..10_000u32 {
            ring.push(round).unwrap();
            assert_eq!(ring.pop(), Ok(round));
        }
        assert!(ring.is_empty());
    }

    #[test]
    fn hands_values_between_threads_in_order() {
        const ITEMS: u32 = 100_000;

        let ring = std::sync::Arc::new(Ring::with_capacity(16));

        let producer = {
            let ring = ring.clone();
            std::thread::spawn(move || {
                for value in 0..ITEMS {
                    loop {
                        match ring.push(value) {
                            Ok(()) => break,
                            Err(rejected) => {
                                assert_eq!(rejected.into_inner(), value);
                                std::thread::yield_now();
                            }
                        }
                    }
                }
            })
        };

        for expected in 0..ITEMS {
            loop {
                match ring.pop() {
                    Ok(value) => {
                        assert_eq!(value, expected);
                        break;
                    }
                    Err(PopError) => std::thread::yield_now(),
                }
            }
        }

        producer.join().unwrap();
        assert!(ring.is_empty());
    }
}
