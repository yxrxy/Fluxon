use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MtimeSizeSignature {
    pub size: u64,
    pub mtime_sec: i64,
    pub mtime_nsec: i64,
}

#[derive(Debug, Error)]
pub enum SignatureError {
    #[error("mtime is before unix epoch")]
    MtimeBeforeEpoch,
}

pub fn signature_from_metadata(
    meta: &std::fs::Metadata,
) -> Result<MtimeSizeSignature, SignatureError> {
    let size = meta.len();
    let mtime = meta
        .modified()
        .map_err(|_| SignatureError::MtimeBeforeEpoch)?;
    let dur = mtime
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SignatureError::MtimeBeforeEpoch)?;
    let sec = dur.as_secs() as i64;
    let nsec = dur.subsec_nanos() as i64;
    Ok(MtimeSizeSignature {
        size,
        mtime_sec: sec,
        mtime_nsec: nsec,
    })
}

pub fn signature_from_system_time_and_size(
    mtime: SystemTime,
    size: u64,
) -> Result<MtimeSizeSignature, SignatureError> {
    let dur = mtime
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SignatureError::MtimeBeforeEpoch)?;
    Ok(MtimeSizeSignature {
        size,
        mtime_sec: dur.as_secs() as i64,
        mtime_nsec: dur.subsec_nanos() as i64,
    })
}
