use std::sync::OnceLock;
use std::time::{Duration, Instant};

use moka::sync::SegmentedCache;

/// Global rate-limit state keyed by string identifier, backed by moka segmented cache.
///
/// Notes:
/// - TTL is fixed to 30 minutes to bound memory; callers still control per-call period.
/// - We intentionally avoid defaults in API; caller must specify period & skip_first explicitly.
static LAST_FIRED: OnceLock<SegmentedCache<String, Instant>> = OnceLock::new();

fn cache() -> &'static SegmentedCache<String, Instant> {
    LAST_FIRED.get_or_init(|| {
        // 64 segments, TTL 30 minutes; capacity is unbounded by default for SegmentedCache
        // (we rely on TTL to evict). If capacity needs to be bounded, set .max_capacity.
        SegmentedCache::builder(64)
            .time_to_live(Duration::from_secs(30 * 60))
            .build()
    })
}

/// Decide whether an action should be allowed under a rate limit.
///
/// - key: unique identity for the rate bucket (e.g. "mpmc:{id}-mpsc:{id}")
/// - period: minimal interval between allowed actions for the same key
/// - skip_first: if true, the very first call for a key returns false (skip),
///               subsequent calls follow the period gate; if false, first call
///               is allowed and records the timestamp.
///
/// Returns true when the caller may proceed (e.g. print a log), false otherwise.
pub fn allow(key: &str, period: Duration, skip_first: bool) -> bool {
    let c = cache();
    let now = Instant::now();
    if let Some(last) = c.get(key) {
        if now.duration_since(last) >= period {
            c.insert(key.to_string(), now);
            true
        } else {
            false
        }
    } else {
        // First observation for this key
        c.insert(key.to_string(), now);
        !skip_first
    }
}
