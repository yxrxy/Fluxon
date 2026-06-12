use anyhow::{Context, Result, anyhow};
use serde_yaml::Value;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::info;

/// Walk up from `start` to filesystem root, returning the first occurrence
/// of `filename` if found.
pub fn find_file_upwards<P: AsRef<Path>>(start: P, filename: &str) -> Option<PathBuf> {
    let mut dir = start.as_ref().to_path_buf();
    loop {
        let candidate = dir.join(filename);
        if candidate.exists() {
            return Some(candidate);
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

/// Best-effort root anchor for config discovery from this crate context.
/// Starts from this crate's manifest dir and prefers the VCS root (.git),
/// then the nearest Cargo workspace root, otherwise falls back to the manifest dir.
pub fn repo_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // Prefer the VCS root when available.
    if let Some(git_root) = find_file_upwards(&manifest_dir, ".git") {
        if let Some(parent) = git_root.parent() {
            return Ok(parent.to_path_buf());
        }
    }

    // Otherwise use the nearest workspace root.
    let mut dir = manifest_dir.clone();
    while dir.parent().is_some() {
        let cargo_toml = dir.join("Cargo.toml");
        if cargo_toml.exists() {
            if let Ok(content) = fs::read_to_string(&cargo_toml) {
                if content.contains("[workspace]") {
                    return Ok(dir);
                }
            }
        }
        dir.pop();
    }

    Ok(manifest_dir)
}

/// Locate `build_config_ext.yml` by walking upwards from the repo/workspace anchor.
pub fn locate_build_ext_config() -> Result<PathBuf> {
    let anchor = repo_root()?;
    if let Some(path) = find_file_upwards(&anchor, "build_config_ext.yml") {
        return Ok(path);
    }
    Err(anyhow!(
        "build_config_ext.yml not found while searching upwards from {:?}",
        anchor
    ))
}

/// Read and parse the build config yaml into a generic serde Value.
pub fn read_build_ext_config_value() -> Result<Value> {
    let path = locate_build_ext_config()?;
    info!("Reading build config: {:?}", path);
    let content = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read build config file: {:?}", path))?;
    let value: Value = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse YAML from: {:?}", path))?;
    Ok(value)
}

/// Read etcd endpoint from build_config_ext.yml and ensure it has http/https scheme.
/// Accepts either a scalar string like "10.126.126.235:2579" or a YAML list of endpoints.
pub fn read_etcd_endpoint_from_build_config() -> Result<String> {
    let v = read_build_ext_config_value()?;
    // Try string first
    if let Some(etcd) = v.get("etcd") {
        if let Some(s) = etcd.as_str() {
            return Ok(normalize_http_prefix(s));
        }
        if let Some(seq) = etcd.as_sequence() {
            // take first string
            if let Some(first) = seq.iter().find_map(|x| x.as_str()) {
                return Ok(normalize_http_prefix(first));
            }
        }
    }
    Err(anyhow!(
        "Missing or invalid 'etcd' in build_config_ext.yml; please set e.g. etcd: 10.126.126.235:2579"
    ))
}

/// Read etcd endpoint from build_config_ext.yml as raw `host:port`.
///
/// This is the authority format for Fluxon KV config fields like
/// `ClientConfig.etcd_addresses_raw` and the `shared.json` external bootstrap contract.
pub fn read_etcd_host_port_from_build_config() -> Result<String> {
    let endpoint = read_etcd_endpoint_from_build_config()?;
    if let Some(rest) = endpoint.strip_prefix("http://") {
        return Ok(rest.to_string());
    }
    if let Some(rest) = endpoint.strip_prefix("https://") {
        return Ok(rest.to_string());
    }
    Ok(endpoint)
}

fn normalize_http_prefix(s: &str) -> String {
    if s.starts_with("http://") || s.starts_with("https://") {
        s.to_string()
    } else {
        format!("http://{}", s)
    }
}

/// Read required `prom_remote_write_url` from build_config_ext.yml.
/// Mirrors setup_and_pack/utils/repo_config_utils.py:load_tsdb_remote_write_url behavior.
pub fn read_prom_remote_write_url_from_build_config() -> Result<String> {
    let v = read_build_ext_config_value()?;
    if let Some(url_val) = v.get("prom_remote_write_url") {
        if let Some(s) = url_val.as_str() {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
        }
    }
    Err(anyhow!(
        "Missing required 'prom_remote_write_url' in build_config_ext.yml"
    ))
}

/// Read TSDB base query URL from build_config_ext.yml, validate scheme/port/path.
/// Name aligned with setup_and_pack/utils/repo_config_utils.py: load_tsdb_base_url.
pub fn load_tsdb_base_url() -> Result<String> {
    let v = read_build_ext_config_value()?;
    let Some(raw) = v
        .get("prom")
        .and_then(|x| x.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    else {
        return Err(anyhow!(
            "build_config_ext.yml 缺少必填字段 'prom'；示例: http://127.0.0.1:9090/api/v1 或 http://127.0.0.1:4000/v1"
        ));
    };
    // Basic scheme and host validation
    if !(raw.starts_with("http://") || raw.starts_with("https://")) {
        return Err(anyhow!(
            "prom 必须以 http/https 开头，示例: http://127.0.0.1:9090/api/v1"
        ));
    }
    // Split netloc and path
    let without_scheme = &raw[raw.find("://").unwrap() + 3..];
    let (netloc, path) = match without_scheme.find('/') {
        Some(idx) => (&without_scheme[..idx], &without_scheme[idx..]),
        None => (without_scheme, ""),
    };
    if path.is_empty() || path == "/" {
        return Err(anyhow!(
            "prom 需要包含路径，示例: http://127.0.0.1:9090/api/v1 或 http://127.0.0.1:4000/v1"
        ));
    }
    if !(path.starts_with("/api/v1") || path.starts_with("/v1")) {
        return Err(anyhow!(
            "prom 的路径应指向查询接口，示例: http://127.0.0.1:9090/api/v1 或 http://127.0.0.1:4000/v1"
        ));
    }
    // Ensure explicit port in netloc
    let port_ok = if netloc.starts_with('[') {
        // IPv6: [::1]:9090
        if let Some(end) = netloc.find("]:") {
            netloc[end + 2..].parse::<u16>().is_ok()
        } else {
            false
        }
    } else {
        // IPv4 or hostname: host:port, expect single ':' for port
        netloc
            .rsplit_once(':')
            .map(|(_, p)| p.parse::<u16>().is_ok())
            .unwrap_or(false)
    };
    if !port_ok {
        return Err(anyhow!(
            "prom 必须显式包含端口，示例: http://127.0.0.1:9090/api/v1 或 http://[::1]:9090/api/v1"
        ));
    }
    Ok(raw.to_string())
}

/// Extract (host, port) from the validated TSDB base URL in build_config_ext.yml.
/// Name aligned with setup_and_pack/utils/repo_config_utils.py: load_tsdb_host_port.
pub fn load_tsdb_host_port() -> Result<(String, u16)> {
    let base = load_tsdb_base_url()?;
    let without_scheme = &base[base.find("://").unwrap() + 3..];
    let netloc = match without_scheme.find('/') {
        Some(idx) => &without_scheme[..idx],
        None => without_scheme,
    };
    if netloc.starts_with('[') {
        // [ipv6]:port
        let end = netloc
            .find("]:")
            .ok_or_else(|| anyhow!("无效 IPv6 host 格式，示例: http://[::1]:9090/api/v1"))?;
        let host = &netloc[1..end];
        let port: u16 = netloc[end + 2..]
            .parse()
            .map_err(|_| anyhow!("端口应为整数: {}", &netloc[end + 2..]))?;
        return Ok((host.to_string(), port));
    }
    // hostname or IPv4: expect host:port
    let (host, port_str) = netloc
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("prom 应为 host:port 形式，示例: http://127.0.0.1:9090/api/v1"))?;
    let port: u16 = port_str
        .parse()
        .map_err(|_| anyhow!("端口应为整数: {}", port_str))?;
    Ok((host.to_string(), port))
}

/// Name aligned with setup_and_pack/utils/repo_config_utils.py: load_tsdb_remote_write_url.
/// Wraps read_prom_remote_write_url_from_build_config for compatibility with Python tooling.
pub fn load_tsdb_remote_write_url() -> Result<String> {
    read_prom_remote_write_url_from_build_config()
}
