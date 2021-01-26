use crate::Stream;
use std::pin::Pin;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::broadcast::Receiver;

use std::fmt;
use std::task::{Context, Poll};

/// A wrapper around [`tokio::sync::broadcast::Receiver`] that implements [`Stream`].
///
/// [`tokio::sync::broadcast::Receiver`]: struct@tokio::sync::broadcast::Receiver
/// [`Stream`]: trait@crate::Stream
pub struct BroadcastStream<T> {
    inner: Receiver<T>
}

/// An error returned from the inner stream of a [`BroadcastStream`].
#[derive(Debug, PartialEq)]
pub enum BroadcastStreamRecvError {
    /// The receiver lagged too far behind. Attempting to receive again will
    /// return the oldest message still retained by the channel.
    ///
    /// Includes the number of skipped messages.
    Lagged(u64),
}

impl<T: Clone + Unpin + 'static + Send + Sync> BroadcastStream<T> {
    /// Create a new `BroadcastStream`.
    pub fn new(rx: Receiver<T>) -> Self {
        Self { inner: rx }
    }
}

impl<T: Clone> Stream for BroadcastStream<T> {
    type Item = Result<T, BroadcastStreamRecvError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.try_recv() {
            Ok(item) => Poll::Ready(Some(Ok(item))),
            Err(TryRecvError::Empty) => Poll::Pending,
            Err(TryRecvError::Closed) => Poll::Ready(None),

            Err(TryRecvError::Lagged(amt)) =>
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(amt)))),
        }
    }
}

impl<T: Clone> fmt::Debug for BroadcastStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BroadcastStream").finish()
    }
}
