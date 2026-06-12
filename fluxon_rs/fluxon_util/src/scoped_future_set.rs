use std::future::Future;
use std::pin::Pin;

use futures::StreamExt;
use futures::stream::FuturesUnordered;

type ScopedFuture<'scope, T> = Pin<Box<dyn Future<Output = T> + Send + 'scope>>;

/// A join-bounded future set that keeps child futures inside the caller scope.
///
/// English note:
/// - Unlike `tokio::task::JoinSet`, this does not detach work onto the runtime and therefore
///   does not widen child futures to `'static`.
/// - Use this when one async path wants fanout/join concurrency over borrowed state that remains
///   owned by the current lexical scope.
/// - Do not use this for background work that may outlive the current scope; keep explicit owned
///   spawn for detached lifetimes.
pub struct ScopedFutureSet<'scope, T> {
    inner: FuturesUnordered<ScopedFuture<'scope, T>>,
}

impl<'scope, T> ScopedFutureSet<'scope, T> {
    pub fn new() -> Self {
        Self {
            inner: FuturesUnordered::new(),
        }
    }

    pub fn push<F>(&mut self, future: F)
    where
        F: Future<Output = T> + Send + 'scope,
    {
        self.inner.push(Box::pin(future));
    }

    pub async fn next(&mut self) -> Option<T> {
        self.inner.next().await
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<'scope, T> Default for ScopedFutureSet<'scope, T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::ScopedFutureSet;

    #[tokio::test]
    async fn scoped_future_set_allows_borrowed_state() {
        let prefix = String::from("prefix");
        let suffix = String::from("suffix");
        let mut running = ScopedFutureSet::new();

        running.push(async { format!("{prefix}-{suffix}") });

        assert_eq!(running.next().await, Some("prefix-suffix".to_string()));
        assert!(running.is_empty());
    }

    #[tokio::test]
    async fn scoped_future_set_returns_when_each_child_completes() {
        let fast = String::from("fast");
        let slow = String::from("slow!");
        let mut running = ScopedFutureSet::new();

        running.push(async {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            slow.len()
        });
        running.push(async { fast.len() });

        let first = running.next().await;
        let second = running.next().await;

        assert_eq!(first, Some(4));
        assert_eq!(second, Some(5));
        assert!(running.next().await.is_none());
    }
}
