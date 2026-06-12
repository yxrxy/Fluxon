use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use tracing_appender::non_blocking;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;
use tracing_subscriber::filter::FilterExt;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::{filter, prelude::*}; // layering

// Build-time generated workspace crate list
mod generated_crates {
    include!(concat!(env!("OUT_DIR"), "/our_crates.rs"));
}

// These RDMA transfer crates carry the runtime evidence we need when diagnosing whether
// RPC fast-path traffic actually entered the closed transfer / verbs backend. Keep the scope explicit:
// only these dependency targets are promoted to DEBUG alongside workspace crates.
const RDMA_DEBUG_TARGETS: &[&str] = &["fabric_lib", "libfabric_sys", "libibverbs_sys"];

// Simple UTC timer in RFC3339 seconds (no subsecond precision)
struct UtcSecondTimer;
impl FormatTime for UtcSecondTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = chrono::Utc::now();
        write!(w, "{}", now.format("%Y-%m-%dT%H:%M:%SZ"))
    }
}

// Keep guards alive for the whole process lifetime to flush non-blocking writers.
static GLOBAL_FILE_LOG_GUARD: OnceLock<WorkerGuard> = OnceLock::new();
static GLOBAL_CONSOLE_LOG_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

// Expose the current process log file path for sidecar collectors (e.g. OTLP tailer).
static GLOBAL_LOG_FILE_PATH: OnceLock<PathBuf> = OnceLock::new();

fn setup_global_log_guards(file_guard: WorkerGuard, console_guard: WorkerGuard) {
    let _ = GLOBAL_FILE_LOG_GUARD.set(file_guard);
    let _ = GLOBAL_CONSOLE_LOG_GUARD.set(console_guard);
}

fn workspace_targets_filter(
    workspace_level: filter::LevelFilter,
    non_workspace_default: filter::LevelFilter,
) -> filter::Targets {
    let mut targets = filter::Targets::new().with_default(non_workspace_default);
    for c in generated_crates::OUR_CRATES {
        targets = targets.with_target(*c, workspace_level);
    }
    for c in RDMA_DEBUG_TARGETS {
        targets = targets.with_target(*c, workspace_level);
    }
    targets
}

fn third_party_log_target_overrides(
    enable_iceoryx_logs: bool,
    default_level: filter::LevelFilter,
) -> filter::Targets {
    let mut targets = filter::Targets::new().with_default(default_level);
    if enable_iceoryx_logs {
        // Narrow scope: only lift suppression for iceoryx2 crates so benchmark logs stay readable.
        for target in [
            "iceoryx2",
            "iceoryx2-bb-concurrency",
            "iceoryx2-bb-container",
            "iceoryx2-bb-derive-macros",
            "iceoryx2-bb-elementary",
            "iceoryx2-bb-elementary-traits",
            "iceoryx2-bb-linux",
            "iceoryx2-bb-lock-free",
            "iceoryx2-bb-memory",
            "iceoryx2-bb-posix",
            "iceoryx2-bb-system-types",
            "iceoryx2-cal",
            "iceoryx2-log",
            "iceoryx2-log-types",
            "iceoryx2-loggers",
            "iceoryx2-pal-concurrency-sync",
            "iceoryx2-pal-configuration",
            "iceoryx2-pal-os-api",
            "iceoryx2-pal-posix",
        ] {
            targets = targets.with_target(target, filter::LevelFilter::DEBUG);
        }
    }
    targets
}

/// Init log for production
/// - `log_path`: directory to write log files
/// - `instance_key`: used in file names to disambiguate instances
pub fn init_log(log_path: &Path, instance_key: &str) {
    init_log_impl(log_path, instance_key, NoopLayer);
}

/// Init log for production, with an extra `tracing_subscriber::Layer` installed.
///
/// This is used by infrastructure crates (e.g. observability) to attach additional sinks
/// such as OTLP log exporters, without introducing crate dependency cycles.
pub fn init_log_with_extra_layer<L>(log_path: &Path, instance_key: &str, extra_layer: L)
where
    L: tracing_subscriber::Layer<Registry> + Send + Sync + 'static,
{
    init_log_impl(log_path, instance_key, extra_layer);
}

#[derive(Clone, Copy, Debug, Default)]
struct NoopLayer;

impl<S> tracing_subscriber::Layer<S> for NoopLayer where S: tracing::Subscriber {}

fn init_log_impl<L>(log_path: &Path, instance_key: &str, extra_layer: L)
where
    L: tracing_subscriber::Layer<Registry> + Send + Sync + 'static,
{
    // Sync FLUXON_LOG <-> RUST_LOG, prefer FLUXON_LOG when both provided.
    let fluxon = std::env::var("FLUXON_LOG").ok();
    let rust_log = std::env::var("RUST_LOG").ok();
    match (fluxon.as_deref(), rust_log.as_deref()) {
        (Some(f), Some(r)) => {
            if f != r {
                eprintln!(
                    "[fluxon] Both FLUXON_LOG='{f}' and RUST_LOG='{r}' set; using FLUXON_LOG"
                );
            }
            unsafe {
                std::env::set_var("RUST_LOG", f);
                std::env::set_var("FLUXON_LOG", f);
            }
        }
        (Some(f), None) => unsafe {
            std::env::set_var("RUST_LOG", f);
            std::env::set_var("FLUXON_LOG", f);
        },
        (None, Some(r)) => unsafe {
            std::env::set_var("FLUXON_LOG", r);
        },
        (None, None) => unsafe {
            // default startup level; callers can override via env before calling
            let default = "debug";
            std::env::set_var("RUST_LOG", default);
            std::env::set_var("FLUXON_LOG", default);
        },
    }

    // Enable backtrace globally
    unsafe {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    // Bridge `log` crate records to `tracing` so external deps honor RUST_LOG
    let _ = tracing_log::LogTracer::init();

    // If RUST_LOG contains a bare level (e.g. "debug"), add our crate targets with that
    // level. If the bare level is debug, downgrade the bare level to info to reduce noise
    // on console, while keeping per-target debug when explicitly requested.
    let current = std::env::var("RUST_LOG").unwrap_or("debug".to_string());
    {
        let raw = current.trim();
        if !raw.is_empty() {
            use std::collections::HashSet;
            let parts: Vec<String> = raw
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();

            let mut existing_targets: HashSet<String> = HashSet::new();
            for p in &parts {
                if let Some((t, _)) = p.split_once('=') {
                    existing_targets.insert(t.trim().to_string());
                }
            }

            let mut crate_level: Option<String> = None;
            let mut new_parts: Vec<String> = Vec::with_capacity(parts.len());
            let mut removed_bare_debug = false;

            for p in parts.into_iter() {
                if p.contains('=') {
                    new_parts.push(p);
                    continue;
                }
                let lvl = p.to_ascii_lowercase();
                if crate_level.is_none() {
                    crate_level = Some(lvl.clone());
                }
                if lvl == "debug" {
                    removed_bare_debug = true; // replace later with bare info
                } else {
                    new_parts.push(p);
                }
            }

            if removed_bare_debug {
                new_parts.push("info".to_string());
            }

            if let Some(level) = crate_level {
                // Use build-time generated workspace member list.
                let our_crates: &[&str] = generated_crates::OUR_CRATES;
                let mut changed = removed_bare_debug;
                for c in our_crates {
                    if !existing_targets.contains(&c.to_string()) {
                        new_parts.push(format!("{}={}", c, level));
                        changed = true;
                    }
                }
                if changed {
                    let merged = new_parts.join(",");
                    unsafe {
                        std::env::set_var("RUST_LOG", &merged);
                    }
                }
            }
        }
    }

    println!(
        "rust_log env: {}",
        std::env::var("RUST_LOG").unwrap_or("info".to_string())
    );

    // Prepare log directory
    match std::fs::create_dir_all(log_path) {
        Ok(_) => {}
        Err(e) => {
            eprintln!(
                "Failed to create log directory {:?}, err: {:?}",
                log_path, e
            );
            return;
        }
    }

    // Archive existing logs for the same instance into a sibling history directory.
    // Scope is strictly within the provided `log_path` (cluster is implied by the dir path),
    // and only files of the current `instance_key` are moved. This avoids any cross-instance
    // interference and keeps behavior explicit and bounded.
    {
        let history_dir = log_path.join("history");
        if let Err(e) = fs::create_dir_all(&history_dir) {
            panic!(
                "[fluxon] Create history directory failed: {:?}. Base log_path: {:?}. \
This log_path is provided by the caller's configuration. \
For Master mode it is derived from MasterConfigYaml.log_dir with a subdirectory '<cluster_name>_cluster_kv_logs'; \
for Client mode it is derived from ClientConfigYaml.fluxonkv_spec.shared_memory_path with subdirectory '<cluster_name>_cluster_kv_logs'. \
Please ensure the directory exists and is writable. Underlying OS error: {:?}",
                history_dir, log_path, e
            );
        }

        // Pattern: fluxon-kv-<instance_key>.<timestamp>.log
        // No fallback patterns: keep rule strict and explicit.
        let prefix = format!("fluxon-kv-{}.", instance_key);
        let mut moved = 0usize;

        let iter = fs::read_dir(log_path).unwrap_or_else(|e| {
            panic!(
                "[fluxon] Read log directory failed at {:?}. This directory is the configured log_path described above. OS error: {:?}",
                log_path, e
            )
        });

        for entry in iter {
            let entry = entry.unwrap_or_else(|e| {
                panic!(
                    "[fluxon] Failed to read a directory entry under {:?}. OS error: {:?}",
                    log_path, e
                )
            });
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let name_os = match path.file_name() {
                Some(n) => n,
                None => continue,
            };
            let name = match name_os.to_str() {
                Some(s) => s,
                None => continue,
            };
            let is_target = name.starts_with(&prefix) && name.ends_with(".log");
            if !is_target {
                continue;
            }
            let dst = history_dir.join(name);
            if let Err(err) = fs::rename(&path, &dst) {
                panic!(
                    "[fluxon] Move old log failed: {:?} -> {:?}. Base log_path: {:?}. OS error: {:?}",
                    path, dst, log_path, err
                );
            }
            moved += 1;
        }

        if moved > 0 {
            println!(
                "[fluxon] Archived {moved} existing logs for instance_key='{instance_key}' into {:?}",
                history_dir
            );
        }
    }

    // Files named with UTC timestamp once per process run
    let ts = chrono::Utc::now().format("%Y-%m-%d_%H-%M-%S");

    // File log keeps workspace crates at DEBUG; non-workspace crates default to WARN.
    // This avoids dumping verbose dependency debug logs (e.g. h2/tower) into file output.
    let file_name = format!("fluxon-kv-{instance_key}.{ts}.log");
    let file_path = log_path.join(&file_name);
    // Keep a copy for the whole process lifetime; collectors can clone it.
    if let Some(prev) = GLOBAL_LOG_FILE_PATH.get() {
        if prev != &file_path {
            eprintln!(
                "[fluxon] init_log called multiple times with different log file paths; keeping the first one: prev={:?}, new={:?}",
                prev, file_path
            );
        }
    } else {
        let _ = GLOBAL_LOG_FILE_PATH.set(file_path.clone());
    }
    let file = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open log file {:?}, err: {:?}", file_path, e);
            return;
        }
    };
    let (file_writer, file_guard) = non_blocking(file);
    let enable_iceoryx_logs = matches!(
        std::env::var("FLUXON_ENABLE_ICEORYX_LOGS")
            .ok()
            .as_deref()
            .map(|v| v.trim().to_ascii_lowercase()),
        Some(v) if !matches!(v.as_str(), "" | "0" | "false" | "no")
    );
    let file_filter =
        workspace_targets_filter(filter::LevelFilter::DEBUG, filter::LevelFilter::WARN);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_timer(UtcSecondTimer)
        .with_writer(file_writer)
        .with_ansi(false)
        .with_filter(file_filter.clone().or(third_party_log_target_overrides(
            enable_iceoryx_logs,
            filter::LevelFilter::WARN,
        )));

    // Console logging follows user config (RUST_LOG/FLUXON_LOG); file logging ignores it.
    let (console_writer, console_guard) = non_blocking(io::stdout());
    let console_env_filter = EnvFilter::from_default_env();
    let console_layer = tracing_subscriber::fmt::layer()
        .with_writer(console_writer)
        .with_filter(console_env_filter);

    // Register layers.
    // `extra_layer` follows the same target filtering as file output (workspace=DEBUG, deps=WARN)
    // to avoid exporting noisy dependency debug logs by default.
    let extra_layer = extra_layer.with_filter(file_filter.clone().or(
        third_party_log_target_overrides(enable_iceoryx_logs, filter::LevelFilter::WARN),
    ));
    let _ = tracing_subscriber::registry()
        .with(extra_layer)
        .with(file_layer)
        .with(console_layer)
        .with(filter::LevelFilter::DEBUG)
        .try_init();

    // Hold guards globally
    setup_global_log_guards(file_guard, console_guard);

    // Success notice: tell users where logs are written.
    let history_dir_for_print = log_path.join("history");
    println!(
        "[fluxon] Logging initialized. base_dir={:?}, history_dir={:?}, instance_key='{}'",
        log_path, history_dir_for_print, instance_key
    );
}

pub fn current_log_file_path() -> Option<PathBuf> {
    GLOBAL_LOG_FILE_PATH.get().cloned()
}

// --- Test helpers ---

fn sanitize_case_name(name: &str) -> String {
    let mut s = name.replace('/', "_").replace('\\', "_").replace(' ', "_");
    if s.is_empty() {
        s = "unnamed_test".to_string();
    }
    s
}

/// Init logging for tests that do not run through run_master/run_client.
/// Writes to: <repo>/log/test_workdir_<unix_ts>/tests/<test_case_name>/
pub fn init_log_test(test_case_name: &str) {
    let case = sanitize_case_name(test_case_name);
    let dir = Path::new(crate::test_util::test_workdir_base())
        .join("tests")
        .join(&case);
    fs::create_dir_all(&dir).expect("create test log dir");
    // Use test_case_name as instance key so file names are recognizable.
    init_log(&dir, &case);
}
