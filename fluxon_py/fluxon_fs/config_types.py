from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import json
from pathlib import Path
from typing import Any, Dict, List

import sys
import yaml

@dataclass(frozen=True)
class FluxonFsMasterPanelConfig:
    listen_addr: str
    public_base_url: str
    auto_refresh_interval_secs: int


class CacheMode(str, Enum):
    DISABLED = "disabled"
    READ_THROUGH = "read_through"


class WriteMode(str, Enum):
    WRITE_BACK = "write_back"
    WRITE_THROUGH = "write_through"


class OnRefreshError(str, Enum):
    APPLY_STALE_WINDOW = "apply_stale_window"
    BYPASS_CACHE_FOR_DIR = "bypass_cache_for_dir"


@dataclass(frozen=True)
class FluxonFsMasterConfig:
    instance_key: str
    pull_interval_ms: int | None


FLUXON_FS_CONTROL_SCHEMA_VERSION = 1
FS_MASTER_CONFIG_RPC_PATH = "/fluxon_fs/config"
FS_MASTER_MOUNT_REGISTRY_RPC_PATH = "/fluxon_fs/mount_registry"
FS_MASTER_EXPORT_REGISTRY_RPC_PATH = "/fluxon_fs/export_registry"
FS_MASTER_BOOTSTRAP_PULL_INTERVAL_MS = 1000
FS_EXPORT_CACHE_BYTES_FIELD_KEY = "bytes"
FS_EXPORT_DEFAULT_METADATA_CACHE_TTL_MS = 5000
FS_CACHE_DEFAULT_WRITE_SESSION_TARGET_INFLIGHT_BYTES = 128 * 1024 * 1024


@dataclass(frozen=True)
class FluxonFsRule:
    dir_abs: str
    cache_mode: CacheMode
    write_mode: WriteMode
    kv_key_prefix: str
    bytes_field_key: str
    max_cache_bytes: int
    on_refresh_error: OnRefreshError


@dataclass(frozen=True)
class FluxonFsGlobalConfig:
    stale_window_ms: int
    write_session_target_inflight_bytes: int
    rules: List[FluxonFsRule]
    exports: Dict[str, "FluxonFsExport"]


@dataclass(frozen=True)
class FluxonFsExportRpcPaths:
    stat: str
    list_dir: str
    read_chunk: str
    write_chunk: str
    truncate: str
    mkdir: str
    rmdir: str
    unlink: str
    rename: str
    chmod: str
    utime: str


@dataclass(frozen=True)
class FluxonFsExport:
    remote_root_dir_abs: str
    routing_mode: "FluxonFsExportRoutingMode"
    nodes: List[str]
    cache_kv_key_prefix: str
    cache_bytes_field_key: str
    cache_max_bytes: int
    metadata_cache_ttl_ms: int
    rpc_paths: FluxonFsExportRpcPaths


class FluxonFsExportRoutingMode(str, Enum):
    STATIC_NODES = "static_nodes"
    AGENT_REGISTRY = "agent_registry"


def export_rpc_paths_for_export_name_v1(export_name: str) -> FluxonFsExportRpcPaths:
    export = _require_str(export_name, "export_name")
    base = f"/fluxon_fs/{export}"
    return FluxonFsExportRpcPaths(
        stat=f"{base}/stat",
        list_dir=f"{base}/list_dir",
        read_chunk=f"{base}/read_chunk",
        write_chunk=f"{base}/write_chunk",
        truncate=f"{base}/truncate",
        mkdir=f"{base}/mkdir",
        rmdir=f"{base}/rmdir",
        unlink=f"{base}/unlink",
        rename=f"{base}/rename",
        chmod=f"{base}/chmod",
        utime=f"{base}/utime",
    )


def export_cache_kv_key_prefix_for_export_name_v1(export_name: str) -> str:
    export = _require_str(export_name, "export_name")
    return f"/fluxon_fs_cache/{export}/"


def export_to_json_text(export: FluxonFsExport) -> str:
    if not isinstance(export, FluxonFsExport):
        raise TypeError(f"export must be FluxonFsExport, got {type(export)}")
    payload: Dict[str, Any] = {
        "remote_root_dir_abs": export.remote_root_dir_abs,
        "routing_mode": export.routing_mode.value,
        "nodes": list(export.nodes),
        "cache_kv_key_prefix": export.cache_kv_key_prefix,
        "cache_bytes_field_key": export.cache_bytes_field_key,
        "cache_max_bytes": int(export.cache_max_bytes),
        "metadata_cache_ttl_ms": int(export.metadata_cache_ttl_ms),
        "rpc_paths": {
            "stat": export.rpc_paths.stat,
            "list_dir": export.rpc_paths.list_dir,
            "read_chunk": export.rpc_paths.read_chunk,
            "write_chunk": export.rpc_paths.write_chunk,
            "truncate": export.rpc_paths.truncate,
            "mkdir": export.rpc_paths.mkdir,
            "rmdir": export.rpc_paths.rmdir,
            "unlink": export.rpc_paths.unlink,
            "rename": export.rpc_paths.rename,
            "chmod": export.rpc_paths.chmod,
            "utime": export.rpc_paths.utime,
        },
    }
    return json.dumps(payload, separators=(",", ":"))


def _require_mapping(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise ValueError(f"{name} must be a mapping")
    return v


def _require_only_keys(v: Dict[str, Any], allowed: set[str], name: str) -> None:
    unknown = sorted(k for k in v.keys() if k not in allowed)
    if unknown:
        raise ValueError(f"{name} contains unknown fields: {', '.join(unknown)}")


def _require_str(v: Any, name: str) -> str:
    if not isinstance(v, str):
        raise ValueError(f"{name} must be a string")
    s = v.strip()
    if not s:
        raise ValueError(f"{name} must be non-empty")
    return s


def _require_int(v: Any, name: str) -> int:
    if not isinstance(v, int):
        raise ValueError(f"{name} must be an int")
    return int(v)


def _require_abs_dir(v: Any, name: str) -> str:
    s = _require_str(v, name)
    p = Path(s)
    if not p.is_absolute():
        raise ValueError(f"{name} must be an absolute path")
    return str(p)


def _require_kv_prefix(v: Any, name: str) -> str:
    s = _require_str(v, name)
    if not s.startswith("/") or not s.endswith("/"):
        raise ValueError(f"{name} must start with '/' and end with '/'")
    return s


def _safe_load_yaml_or_print(raw: str, *, source: str) -> Any:
    try:
        return yaml.safe_load(raw)
    except yaml.YAMLError:
        # English note: PyYAML exceptions often show only a snippet; print the full document for debugging.
        print(
            f"YAML parse failed: source={source}\n--- YAML BEGIN ---\n{raw}\n--- YAML END ---",
            file=sys.stderr,
        )
        raise


def parse_master_config_from_file(path: Path) -> FluxonFsMasterConfig:
    raw = path.read_text(encoding="utf-8")
    loaded = _safe_load_yaml_or_print(raw, source=str(path))
    top = _require_mapping(loaded, "config")
    fs = _require_mapping(top.get("fluxon_fs"), "fluxon_fs")
    if "rpc" in fs:
        raise ValueError("fluxon_fs.rpc is removed; use fluxon_fs.master")
    master = _require_mapping(fs.get("master"), "fluxon_fs.master")

    if "rpc_timeout_ms" in master:
        raise ValueError(
            "fluxon_fs.master.rpc_timeout_ms is removed; Fluxon user-RPC timeout defaults to 10000ms per call"
        )
    _require_only_keys(master, {"instance_key", "pull_interval_ms"}, "fluxon_fs.master")

    pull_interval_ms: int | None = None
    if "pull_interval_ms" in master:
        pull_interval_ms = _require_int(
            master.get("pull_interval_ms"), "fluxon_fs.master.pull_interval_ms"
        )
        if pull_interval_ms <= 0:
            raise ValueError("fluxon_fs.master.pull_interval_ms must be > 0")

    return FluxonFsMasterConfig(
        instance_key=_require_str(master.get("instance_key"), "fluxon_fs.master.instance_key"),
        pull_interval_ms=pull_interval_ms,
    )


def parse_master_panel_config_from_file(path: Path) -> FluxonFsMasterPanelConfig:
    raw = path.read_text(encoding="utf-8")
    loaded = _safe_load_yaml_or_print(raw, source=str(path))
    top = _require_mapping(loaded, "config")
    fs = _require_mapping(top.get("fluxon_fs"), "fluxon_fs")
    panel = _require_mapping(fs.get("master_panel"), "fluxon_fs.master_panel")

    listen_addr = _require_str(panel.get("listen_addr"), "fluxon_fs.master_panel.listen_addr")
    if ":" not in listen_addr:
        raise ValueError("fluxon_fs.master_panel.listen_addr must be 'host:port'")

    public_base_url = _require_str(panel.get("public_base_url"), "fluxon_fs.master_panel.public_base_url")
    if "://" not in public_base_url:
        raise ValueError("fluxon_fs.master_panel.public_base_url must be http(s)://..")

    auto_refresh_interval_secs = _require_int(
        panel.get("auto_refresh_interval_secs"),
        "fluxon_fs.master_panel.auto_refresh_interval_secs",
    )
    if auto_refresh_interval_secs <= 0:
        raise ValueError("fluxon_fs.master_panel.auto_refresh_interval_secs must be > 0")

    return FluxonFsMasterPanelConfig(
        listen_addr=listen_addr,
        public_base_url=public_base_url.rstrip("/"),
        auto_refresh_interval_secs=auto_refresh_interval_secs,
    )


def extract_global_config_yaml_from_file(path: Path) -> str:
    raw = path.read_text(encoding="utf-8")
    loaded = _safe_load_yaml_or_print(raw, source=str(path))
    top = _require_mapping(loaded, "config")
    fs = _require_mapping(top.get("fluxon_fs"), "fluxon_fs")
    cache = _require_mapping(fs.get("cache"), "fluxon_fs.cache")
    return yaml.safe_dump(cache, sort_keys=True)


def parse_global_config_from_yaml_text(text: str) -> FluxonFsGlobalConfig:
    loaded = _safe_load_yaml_or_print(text, source="<fluxon_fs.cache yaml text>")
    cache = _require_mapping(loaded, "fluxon_fs.cache")
    _require_only_keys(
        cache,
        {"stale_window_ms", "write_session_target_inflight_bytes", "rules", "exports"},
        "fluxon_fs.cache",
    )

    stale_window_ms = _require_int(cache.get("stale_window_ms"), "fluxon_fs.cache.stale_window_ms")
    if stale_window_ms <= 0:
        raise ValueError("fluxon_fs.cache.stale_window_ms must be > 0")
    write_session_target_inflight_bytes = _require_int(
        cache.get(
            "write_session_target_inflight_bytes",
            FS_CACHE_DEFAULT_WRITE_SESSION_TARGET_INFLIGHT_BYTES,
        ),
        "fluxon_fs.cache.write_session_target_inflight_bytes",
    )
    if write_session_target_inflight_bytes <= 0:
        raise ValueError("fluxon_fs.cache.write_session_target_inflight_bytes must be > 0")
    raw_rules = cache.get("rules", [])
    if not isinstance(raw_rules, list):
        raise ValueError("fluxon_fs.cache.rules must be a list")

    rules: List[FluxonFsRule] = []
    for i, r in enumerate(raw_rules):
        rr = _require_mapping(r, f"fluxon_fs.cache.rules[{i}]")
        _require_only_keys(
            rr,
            {
                "dir_abs",
                "cache_mode",
                "write_mode",
                "kv_key_prefix",
                "bytes_field_key",
                "max_cache_bytes",
                "on_refresh_error",
            },
            f"fluxon_fs.cache.rules[{i}]",
        )
        cache_mode = CacheMode(_require_str(rr.get("cache_mode"), f"rules[{i}].cache_mode").lower())
        write_mode = WriteMode(_require_str(rr.get("write_mode"), f"rules[{i}].write_mode").lower())
        on_refresh_error = OnRefreshError(
            _require_str(rr.get("on_refresh_error"), f"rules[{i}].on_refresh_error").lower()
        )
        max_cache_bytes = _require_int(rr.get("max_cache_bytes"), f"rules[{i}].max_cache_bytes")
        if max_cache_bytes <= 0:
            raise ValueError(f"rules[{i}].max_cache_bytes must be > 0")

        rules.append(
            FluxonFsRule(
                dir_abs=_require_abs_dir(rr.get("dir_abs"), f"rules[{i}].dir_abs"),
                cache_mode=cache_mode,
                write_mode=write_mode,
                kv_key_prefix=_require_kv_prefix(rr.get("kv_key_prefix"), f"rules[{i}].kv_key_prefix"),
                bytes_field_key=_require_str(rr.get("bytes_field_key"), f"rules[{i}].bytes_field_key"),
                max_cache_bytes=max_cache_bytes,
                on_refresh_error=on_refresh_error,
            )
        )

    raw_exports = cache.get("exports")
    if not isinstance(raw_exports, dict):
        raise ValueError("fluxon_fs.cache.exports must be a mapping")

    exports: Dict[str, FluxonFsExport] = {}
    for name, v in raw_exports.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError("fluxon_fs.cache.exports keys must be non-empty strings")
        ev = _require_mapping(v, f"fluxon_fs.cache.exports[{name!r}]")
        if "rpc_timeout_ms" in ev:
            raise ValueError(f"exports[{name}].rpc_timeout_ms is removed; Fluxon user-RPC timeout defaults to 10000ms per call")
        if "rpc_paths" in ev:
            raise ValueError(
                f"exports[{name}].rpc_paths is removed; RPC paths are derived from export name"
            )
        _require_only_keys(
            ev,
            {"remote_root_dir_abs", "nodes", "cache_max_bytes", "metadata_cache_ttl_ms"},
            f"fluxon_fs.cache.exports[{name!r}]",
        )
        remote_root = _require_abs_dir(ev.get("remote_root_dir_abs"), f"exports[{name}].remote_root_dir_abs")
        nodes_any = ev.get("nodes")
        nodes: List[str] = []
        if nodes_any is None:
            routing_mode = FluxonFsExportRoutingMode.AGENT_REGISTRY
        elif isinstance(nodes_any, list):
            if len(nodes_any) == 0:
                raise ValueError(
                    f"exports[{name}].nodes must be non-empty when provided"
                )
            for i, n in enumerate(nodes_any):
                nodes.append(_require_str(n, f"exports[{name}].nodes[{i}]"))
            routing_mode = FluxonFsExportRoutingMode.STATIC_NODES
        else:
            raise ValueError(f"exports[{name}].nodes must be a list when provided")

        cache_max_bytes = _require_int(ev.get("cache_max_bytes"), f"exports[{name}].cache_max_bytes")
        if cache_max_bytes <= 0:
            raise ValueError(f"exports[{name}].cache_max_bytes must be > 0")
        metadata_cache_ttl_ms_raw = ev.get("metadata_cache_ttl_ms", FS_EXPORT_DEFAULT_METADATA_CACHE_TTL_MS)
        metadata_cache_ttl_ms = _require_int(
            metadata_cache_ttl_ms_raw,
            f"exports[{name}].metadata_cache_ttl_ms",
        )
        if metadata_cache_ttl_ms <= 0:
            raise ValueError(f"exports[{name}].metadata_cache_ttl_ms must be > 0")
        rpc_paths = export_rpc_paths_for_export_name_v1(name)
        exports[name] = FluxonFsExport(
            remote_root_dir_abs=remote_root,
            routing_mode=routing_mode,
            nodes=nodes,
            cache_kv_key_prefix=export_cache_kv_key_prefix_for_export_name_v1(name),
            cache_bytes_field_key=FS_EXPORT_CACHE_BYTES_FIELD_KEY,
            cache_max_bytes=cache_max_bytes,
            metadata_cache_ttl_ms=metadata_cache_ttl_ms,
            rpc_paths=rpc_paths,
        )

    return FluxonFsGlobalConfig(
        stale_window_ms=stale_window_ms,
        write_session_target_inflight_bytes=write_session_target_inflight_bytes,
        rules=rules,
        exports=exports,
    )
