//! Error types: a full buffer hands the value back, an empty buffer reports
//! the miss.

use core::fmt;

/// Returned by [`Ring::push`](crate::Ring::push) when the buffer is full.
///
/// The rejected value travels inside the error so no allocation or clone is
/// needed to retry it later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushError<T>(pub T);

impl<T> PushError<T> {
    /// Unwraps the value that could not be pushed.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: fmt::Debug> fmt::Display for PushError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ring buffer is full, rejected {:?}", self.0)
    }
}

impl<T: fmt::Debug> std::error::Error for PushError<T> {}

/// Returned by [`Ring::pop`](crate::Ring::pop) when the buffer is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PopError;

impl fmt::Display for PopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ring buffer is empty")
    }
}

impl std::error::Error for PopError {}
