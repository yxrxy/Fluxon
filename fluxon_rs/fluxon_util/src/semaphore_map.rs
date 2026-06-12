use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

/// A lightweight per-key semaphore manager backed by a sharded async Moka cache.
///
/// - Each key maps to a `tokio::sync::Semaphore` with a fixed number of permits.
/// - Use `acquire(key)` to get an `OwnedSemaphorePermit` for that key.
/// - The cache is sharded by Moka; construction uses lazy `get_with`.
pub struct SemaphoreMap<K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    sems: moka::sync::SegmentedCache<K, Arc<tokio::sync::Semaphore>>,
    permits_per_key: usize,
}

impl<K> SemaphoreMap<K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Create a new keyed semaphore manager.
    /// - `permits_per_key`: semaphore permits per key (use 1 for single-inflight per key).
    /// - `ttl`: cache entry TTL; recommended to be ~2x the expected max inflight duration.
    pub fn new(permits_per_key: usize, ttl: Duration) -> Self {
        let sems = moka::sync::SegmentedCache::builder(64)
            .time_to_live(ttl)
            .build();
        Self {
            sems,
            permits_per_key,
        }
    }

    /// Acquire a permit for the given key, creating the semaphore lazily if needed.
    pub async fn acquire(&self, key: K) -> tokio::sync::OwnedSemaphorePermit {
        let permits = self.permits_per_key;
        // Lazily create or fetch the semaphore for this key.
        let sem = self
            .sems
            .get_with(key, || Arc::new(tokio::sync::Semaphore::new(permits)));
        // Safe to unwrap: we never close these semaphores; a failure indicates a logic bug.
        sem.clone()
            .acquire_owned()
            .await
            .expect("SemaphoreMap: acquire should never fail")
    }
}
