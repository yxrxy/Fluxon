use std::time::{Duration, Instant};

pub const DEFAULT_WARN_INTERVAL_SECS: u64 = 15;

#[derive(Debug, Clone, Copy)]
pub struct BackoffConfig {
    pub initial_secs: u64,
    pub max_secs: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct WarnConfig {
    pub warn_interval_secs: u64,
}

pub fn next_backoff(cfg: BackoffConfig, attempt: u32) -> Duration {
    let shift = attempt.min(31);
    let mul = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
    let mut secs = cfg.initial_secs.saturating_mul(mul);
    if secs > cfg.max_secs {
        secs = cfg.max_secs;
    }
    Duration::from_secs(secs)
}

pub fn should_warn(now: Instant, last_warn: &mut Option<Instant>, cfg: WarnConfig) -> bool {
    let Some(prev) = last_warn.as_ref() else {
        *last_warn = Some(now);
        return true;
    };
    if now.duration_since(*prev).as_secs() >= cfg.warn_interval_secs {
        *last_warn = Some(now);
        return true;
    }
    false
}
