#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Any, Dict

import yaml

from fluxon_py.config import FluxonKvClientConfig
from fluxon_py.kvclient import new_store
from fluxon_py.logging import init_logger
from fluxon_py.tool import import_fluxon_pyo3_local

from fluxon_py.fluxon_fs.config_types import extract_global_config_yaml_from_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Fluxon FS demo writer")
    parser.add_argument("-c", "--config", required=True, help="YAML config file")
    parser.add_argument("-w", "--workdir", required=True, help="Workdir")
    args = parser.parse_args()

    config_path = Path(args.config)
    workdir = Path(args.workdir)
    if not config_path.exists():
        raise FileNotFoundError(f"config not found: {config_path}")
    workdir.mkdir(parents=True, exist_ok=True)

    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("config file must be a mapping")

    kv_cfg = _require_mapping(loaded.get("kvclient"), "kvclient")
    demo = _require_mapping(loaded.get("fluxon_fs_demo"), "fluxon_fs_demo")

    remote_export_root_dir_abs = _require_abs_dir(
        demo.get("remote_export_root_dir_abs"), "fluxon_fs_demo.remote_export_root_dir_abs"
    )
    remote_relpath = _require_relpath(demo.get("remote_relpath"), "fluxon_fs_demo.remote_relpath")

    local_root_dir_abs = _require_abs_dir(demo.get("local_root_dir_abs"), "fluxon_fs_demo.local_root_dir_abs")
    local_relpath = _require_relpath(demo.get("local_relpath"), "fluxon_fs_demo.local_relpath")

    interval_ms = _require_int(demo.get("interval_ms"), "fluxon_fs_demo.interval_ms")
    small_bytes = _require_int(demo.get("small_bytes"), "fluxon_fs_demo.small_bytes")
    large_bytes = _require_int(demo.get("large_bytes"), "fluxon_fs_demo.large_bytes")

    init_logger("fluxon_fs_demo_writer")

    store = new_store(FluxonKvClientConfig(kv_cfg)).unwrap("new_store failed")
    try:
        inner = getattr(store, "_client", None)
        if inner is None:
            raise RuntimeError("expected fluxon kvclient store to expose _client")

        cache_yaml = extract_global_config_yaml_from_file(config_path)
        fluxon_pyo3 = import_fluxon_pyo3_local()
        reg = fluxon_pyo3.fluxon_fs_register_agent(inner, str(cache_yaml))
        if not reg.is_ok():
            raise RuntimeError(f"fluxon_fs_register_agent failed: {reg.unwrap_error()}")
        reg.unwrap()

        remote_root = Path(remote_export_root_dir_abs)
        remote_root.mkdir(parents=True, exist_ok=True)
        remote_file = _safe_join(remote_root, remote_relpath)
        remote_file.parent.mkdir(parents=True, exist_ok=True)

        local_root = Path(local_root_dir_abs)
        local_root.mkdir(parents=True, exist_ok=True)
        local_file = _safe_join(local_root, local_relpath)
        local_file.parent.mkdir(parents=True, exist_ok=True)

        print(
            f"[writer] started instance_key={store.instance_key().unwrap()} remote_file={remote_file} local_file={local_file}",
            flush=True,
        )

        seq = 0
        while True:
            # Remote file: alternate small/large to exercise KV-cache vs mirror path.
            remote_size = small_bytes if (seq % 2 == 0) else large_bytes
            _write_one(
                op="write_remote",
                dst=remote_file,
                payload=_build_payload(seq=seq, now_ns=time.time_ns(), size=remote_size, tag="remote"),
            )

            # Local file: always small to remain within local cache rule max_cache_bytes.
            _write_one(
                op="write_local",
                dst=local_file,
                payload=_build_payload(seq=seq, now_ns=time.time_ns(), size=small_bytes, tag="local"),
            )

            seq += 1
            time.sleep(interval_ms / 1000.0)
    finally:
        close_res = store.close()
        if not close_res.is_ok():
            raise RuntimeError(f"close failed: {close_res.unwrap_error()}")
        close_res.unwrap()


def _write_one(*, op: str, dst: Path, payload: bytes) -> None:
    t0 = time.monotonic_ns()
    tmp = dst.with_suffix(dst.suffix + f".tmp.{os.getpid()}")
    tmp.write_bytes(payload)
    os.replace(str(tmp), str(dst))
    dt_ms = (time.monotonic_ns() - t0) / 1_000_000.0
    print(f"[writer] op={op} path={dst} bytes={len(payload)} elapsed_ms={dt_ms:.3f}", flush=True)


def _require_mapping(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise ValueError(f"{name} must be a mapping")
    return v


def _require_int(v: Any, name: str) -> int:
    if not isinstance(v, int):
        raise ValueError(f"{name} must be an int")
    return int(v)


def _require_abs_dir(v: Any, name: str) -> str:
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"{name} must be a non-empty string")
    p = Path(v)
    if not p.is_absolute():
        raise ValueError(f"{name} must be an absolute path")
    return str(p)


def _require_relpath(v: Any, name: str) -> str:
    if not isinstance(v, str):
        raise ValueError(f"{name} must be a string")
    s = v.replace("\\", "/")
    while s.startswith("/"):
        s = s[1:]
    if not s or s == ".":
        raise ValueError(f"{name} must be a non-empty relative path")
    parts = [x for x in s.split("/") if x not in ("", ".")]
    if any(x == ".." for x in parts):
        raise ValueError(f"{name} must not contain '..'")
    return "/".join(parts)


def _safe_join(root: Path, relpath: str) -> Path:
    root_r = root.resolve()
    p = (root_r / relpath).resolve()
    if root_r != p and root_r not in p.parents:
        raise ValueError("relpath escapes root")
    return p


def _build_payload(*, seq: int, now_ns: int, size: int, tag: str) -> bytes:
    if size < 0:
        raise ValueError("size must be non-negative")
    header = f"tag={tag} seq={seq} now_ns={now_ns} size={size}\n".encode("ascii")
    if len(header) > size:
        raise ValueError("payload size is too small for header")
    body = b"A" * (size - len(header))
    return header + body


if __name__ == "__main__":
    main()
