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

from fluxon_py.fluxon_fs.bootstrap import install_patcher_from_master


def main() -> None:
    parser = argparse.ArgumentParser(description="Fluxon FS demo reader")
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

    mount_dir_abs = _require_abs_dir(demo.get("mount_dir_abs"), "fluxon_fs_demo.mount_dir_abs")
    export_name = _require_str(demo.get("export_name"), "fluxon_fs_demo.export_name")
    remote_relpath = _require_relpath(demo.get("remote_relpath"), "fluxon_fs_demo.remote_relpath")

    local_root_dir_abs = _require_abs_dir(demo.get("local_root_dir_abs"), "fluxon_fs_demo.local_root_dir_abs")
    local_relpath = _require_relpath(demo.get("local_relpath"), "fluxon_fs_demo.local_relpath")

    interval_ms = _require_int(demo.get("interval_ms"), "fluxon_fs_demo.interval_ms")
    wait_file_poll_ms = _require_int(demo.get("wait_file_poll_ms"), "fluxon_fs_demo.wait_file_poll_ms")
    print_limit_bytes = _require_int(demo.get("print_limit_bytes"), "fluxon_fs_demo.print_limit_bytes")

    init_logger("fluxon_fs_demo_reader")

    store = new_store(FluxonKvClientConfig(kv_cfg)).unwrap("new_store failed")
    patcher = None
    try:
        patcher = install_patcher_from_master(kv_store=store, config_path=config_path)
        patcher.wait_cache_config_loaded()
        patcher.mount_remote_dir(
            local_mount_dir_abs=mount_dir_abs,
            export_name=export_name,
        )

        remote_file_abs = os.path.join(mount_dir_abs, remote_relpath)
        local_file_abs = os.path.join(local_root_dir_abs, local_relpath)

        print(
            f"[reader] started instance_key={store.instance_key().unwrap()} remote_file={remote_file_abs} local_file={local_file_abs}",
            flush=True,
        )

        # Ensure both files exist before entering the fast loop.
        while not os.path.exists(remote_file_abs):
            print(f"[reader] op=wait_remote_exists path={remote_file_abs}", flush=True)
            time.sleep(wait_file_poll_ms / 1000.0)
        while not os.path.exists(local_file_abs):
            print(f"[reader] op=wait_local_exists path={local_file_abs}", flush=True)
            time.sleep(wait_file_poll_ms / 1000.0)

        seq = 0
        while True:
            if seq % 2 == 0:
                _read_one(op="read_remote", path=remote_file_abs, print_limit_bytes=print_limit_bytes)
            else:
                _read_one(op="read_local", path=local_file_abs, print_limit_bytes=print_limit_bytes)
            seq += 1
            time.sleep(interval_ms / 1000.0)
    finally:
        if patcher is not None:
            patcher.uninstall()
        close_res = store.close()
        if not close_res.is_ok():
            raise RuntimeError(f"close failed: {close_res.unwrap_error()}")
        close_res.unwrap()


def _read_one(*, op: str, path: str, print_limit_bytes: int) -> None:
    t0 = time.monotonic_ns()
    with open(path, "rb") as f:
        data = f.read()
    dt_ms = (time.monotonic_ns() - t0) / 1_000_000.0

    prefix = data[:print_limit_bytes]
    try:
        prefix_s = prefix.decode("utf-8")
    except UnicodeDecodeError:
        prefix_s = prefix.decode("utf-8", errors="replace")

    print(
        f"[reader] op={op} path={path} bytes={len(data)} elapsed_ms={dt_ms:.3f} prefix=\n{prefix_s}",
        flush=True,
    )


def _require_mapping(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise ValueError(f"{name} must be a mapping")
    return v


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


if __name__ == "__main__":
    main()
