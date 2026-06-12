use anyhow::Result;
use fluxon_commu_contract::{RdmaProbeSnapshot, RdmaRuntimeSnapshot};
use std::collections::BTreeSet;
use std::env;
use std::fs;

fn trim_to_option(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn read_env_optional(name: &str) -> Option<String> {
    env::var(name).ok().and_then(trim_to_option)
}

fn read_link_optional(path: &str) -> Option<String> {
    fs::read_link(path)
        .ok()
        .and_then(|value| trim_to_option(value.display().to_string()))
}

fn current_dir_optional() -> Option<String> {
    env::current_dir()
        .ok()
        .and_then(|value| trim_to_option(value.display().to_string()))
}

fn read_cmdline() -> Vec<String> {
    let Ok(raw) = fs::read("/proc/self/cmdline") else {
        return Vec::new();
    };
    raw.split(|byte| *byte == 0)
        .filter_map(|chunk| {
            if chunk.is_empty() {
                return None;
            }
            std::str::from_utf8(chunk)
                .ok()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
        .collect()
}

fn read_namespace_links() -> Vec<String> {
    let mut namespace_links = Vec::new();
    for namespace in ["mnt", "pid", "net", "user", "ipc", "uts", "cgroup"] {
        let path = format!("/proc/self/ns/{namespace}");
        if let Some(target) = read_link_optional(&path) {
            namespace_links.push(format!("{namespace}={target}"));
        }
    }
    namespace_links
}

fn read_relevant_loaded_libraries() -> Vec<String> {
    let Ok(maps) = fs::read_to_string("/proc/self/maps") else {
        return Vec::new();
    };
    let mut relevant_entries = BTreeSet::new();
    let needles = [
        "fluxon_pyo3",
        "libfluxon_rdma_probe",
        "libibverbs",
        "librdmacm",
        "libmlx5",
        "libmlx",
        "libefa",
        "libfabric",
        "libfluxon_commu_core",
    ];
    for line in maps.lines() {
        if needles.iter().any(|needle| line.contains(needle)) {
            relevant_entries.insert(line.to_string());
        }
    }
    relevant_entries.into_iter().collect()
}

pub fn capture_rdma_runtime_snapshot() -> RdmaRuntimeSnapshot {
    RdmaRuntimeSnapshot {
        pid: std::process::id(),
        ppid: unsafe { libc::getppid() as u32 },
        exe: read_link_optional("/proc/self/exe"),
        cwd: current_dir_optional(),
        root: read_link_optional("/proc/self/root"),
        cmdline: read_cmdline(),
        namespace_links: read_namespace_links(),
        env_fluxon_pyo3_libs_dir: read_env_optional("FLUXON_PYO3_LIBS_DIR"),
        env_rdmav_drivers: read_env_optional("RDMAV_DRIVERS"),
        env_ibv_drivers: read_env_optional("IBV_DRIVERS"),
        env_ld_library_path: read_env_optional("LD_LIBRARY_PATH"),
        relevant_loaded_libraries: read_relevant_loaded_libraries(),
    }
}

pub fn probe_rdma_snapshot() -> Result<RdmaProbeSnapshot> {
    Ok(RdmaProbeSnapshot {
        ports: Vec::new(),
        probe_error: Some(
            "rdma probe runtime moved behind the bundled closed-runtime boundary; public surface exposes runtime snapshot only"
                .to_string(),
        ),
        verbs_device_count: 0,
        ibv_get_device_list_device_count_raw: 0,
        ibv_get_device_list_returned_null: false,
        ibv_get_device_list_errno: None,
        verbs_device_names: Vec::new(),
        sysfs_infiniband_entries: Vec::new(),
        dev_infiniband_entries: Vec::new(),
        env_rdmav_drivers: None,
        env_ibv_drivers: None,
        env_ld_library_path: None,
        runtime_snapshot: capture_rdma_runtime_snapshot(),
    })
}
