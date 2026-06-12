use anyhow::{Result, anyhow};
use limit_thirdparty::tokio::time::sleep;
use std::fs::Metadata;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::time::Duration;

/// Lightweight file change signature based on inode and mtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileSignature {
    pub inode: u64,
    pub mtime_sec: i64,
    pub mtime_nsec: i64,
}

impl FileSignature {
    #[inline]
    pub fn from_metadata(meta: &Metadata) -> Self {
        #[cfg(unix)]
        {
            Self {
                inode: meta.ino(),
                mtime_sec: meta.mtime(),
                mtime_nsec: meta.mtime_nsec(),
            }
        }
        #[cfg(not(unix))]
        {
            // Fallback: use file length and modified timestamp seconds
            use std::time::UNIX_EPOCH;
            let m = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            Self {
                inode: 0,
                mtime_sec: m,
                mtime_nsec: 0,
            }
        }
    }
}

/// Read the current signature of a file.
pub fn get_file_signature(path: &str) -> Result<FileSignature> {
    let meta =
        std::fs::metadata(path).map_err(|e| anyhow!("Failed to stat file '{}': {}", path, e))?;
    Ok(FileSignature::from_metadata(&meta))
}

/// Compare a file's signature with a previous one. Returns Some(new_sig) if changed, None if unchanged.
pub fn signature_changed(
    path: &str,
    prev: Option<&FileSignature>,
) -> Result<Option<FileSignature>> {
    let sig = get_file_signature(path)?;
    let changed = match prev {
        Some(p) => p != &sig,
        None => true,
    };
    if changed { Ok(Some(sig)) } else { Ok(None) }
}

/// Check if all required files exist inside `dir`.
pub fn are_files_ready(dir: &str, files: &[&str]) -> bool {
    files
        .iter()
        .all(|f| std::path::Path::new(&format!("{}/{}", dir, f)).exists())
}

/// Generic readiness: caller supplies required file names.
/// Note: if you need non-empty semantics, include a validator upstream.

/// Wait until all `files` exist inside `dir` (polling).
/// Times out after `max_wait_secs` seconds.
pub async fn wait_for_files_ready(dir: &str, files: &[&str], max_wait_secs: u64) -> Result<()> {
    // Ensure directory exists (best effort)
    let _ = std::fs::create_dir_all(dir);

    if are_files_ready(dir, files) {
        return Ok(());
    }

    // Polling loop
    let sleep_ms = 300u64;
    // ceil division to avoid rounding down
    let max_iters = (max_wait_secs.saturating_mul(1000) + (sleep_ms - 1)) / sleep_ms;
    let mut iters = 0u64;
    loop {
        if are_files_ready(dir, files) {
            return Ok(());
        }
        sleep(Duration::from_millis(sleep_ms)).await;
        iters += 1;
        if iters >= max_iters {
            return Err(anyhow!(
                "Timeout waiting for files {:?} in {} after {}s",
                files,
                dir,
                max_wait_secs
            ));
        }
    }
}

/// Wait until `shared.json` (non-empty) and `mmap.file` are ready inside `dir` (polling).
// Removed specialized `wait_for_shared_memory_files_ready`; use `wait_for_files_ready` instead.

#[cfg(test)]
mod tests {
    use super::*;
    use limit_thirdparty::tokio;
    use std::fs;

    #[tokio::test]
    async fn test_wait_for_files_ready_creates_and_detects() {
        let dir = tempfile::tempdir().unwrap();
        let path_str = dir.path().to_str().unwrap().to_string();
        let f1 = "shared.json";
        let f2 = "mmap.file";

        // Spawn a task that creates files with delays
        let p = path_str.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            fs::write(format!("{}/{}", p, f1), b"{\"segment_len\":1}").unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            fs::write(format!("{}/{}", p, f2), b"mmapped").unwrap();
        });

        // Should complete before timeout
        wait_for_files_ready(&path_str, &[f1, f2], 5).await.unwrap();
        // Repeat with the generic API again
        wait_for_files_ready(&path_str, &[f1, f2], 5).await.unwrap();
    }
}
