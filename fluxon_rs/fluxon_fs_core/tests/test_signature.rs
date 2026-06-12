use fluxon_fs_core::signature::{SignatureError, signature_from_system_time_and_size};

use std::time::{Duration, UNIX_EPOCH};

#[test]
fn signature_rejects_mtime_before_unix_epoch() {
    let t = UNIX_EPOCH - Duration::from_secs(1);
    let err = signature_from_system_time_and_size(t, 1).unwrap_err();
    assert!(matches!(err, SignatureError::MtimeBeforeEpoch));
}

#[test]
fn signature_accepts_epoch_and_extracts_fields() {
    let t = UNIX_EPOCH + Duration::from_nanos(123);
    let sig = signature_from_system_time_and_size(t, 42).unwrap();
    assert_eq!(sig.size, 42);
    assert_eq!(sig.mtime_sec, 0);
    assert_eq!(sig.mtime_nsec, 123);
}

#[test]
fn signature_rounds_trip_seconds_and_nanos() {
    let t = UNIX_EPOCH + Duration::from_secs(10) + Duration::from_nanos(999);
    let sig = signature_from_system_time_and_size(t, 0).unwrap();
    assert_eq!(sig.mtime_sec, 10);
    assert_eq!(sig.mtime_nsec, 999);
}
