use fluxon_fs_core::retry::{BackoffConfig, WarnConfig, next_backoff, should_warn};

use std::time::{Duration, Instant};

#[test]
fn next_backoff_monotonic_and_capped() {
    let cfg = BackoffConfig {
        initial_secs: 5,
        max_secs: 30,
    };

    let d0 = next_backoff(cfg, 0);
    let d1 = next_backoff(cfg, 1);
    let d2 = next_backoff(cfg, 2);
    let d10 = next_backoff(cfg, 10);

    assert_eq!(d0, Duration::from_secs(5));
    assert!(d1 >= d0);
    assert!(d2 >= d1);
    assert_eq!(d10, Duration::from_secs(30));
}

#[test]
fn should_warn_respects_interval() {
    let cfg = WarnConfig {
        warn_interval_secs: 10,
    };
    let t0 = Instant::now();

    let mut last: Option<Instant> = None;
    assert!(should_warn(t0, &mut last, cfg));
    assert!(last.is_some());

    let t1 = t0 + Duration::from_secs(9);
    assert!(!should_warn(t1, &mut last, cfg));

    let t2 = t0 + Duration::from_secs(10);
    assert!(should_warn(t2, &mut last, cfg));
}
