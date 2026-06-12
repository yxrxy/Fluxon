use std::path::PathBuf;
use std::sync::OnceLock;
use std::fs;

/// Return per-run test workdir base under repo log folder.
/// Format: <repo>/log/test_workdir_YYYY_MM_DD_HH_MM_SS
pub fn test_workdir_base() -> &'static str {
    static TEST_WORKDIR: OnceLock<String> = OnceLock::new();
    TEST_WORKDIR.get_or_init(|| {
        // Keep consistent with prior behavior of writing under ../../log from each crate
        let mut base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        base.push("../../log");

        // Human-friendly timestamp to the second (UTC)
        let ts = chrono::Utc::now().format("%Y_%m_%d_%H_%M_%S");
        base.push(format!("test_workdir_{}", ts));

        fs::create_dir_all(&base).expect("create test base workdir");
        base.to_string_lossy().to_string()
    })
}

