#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
from pathlib import Path
import yaml

from fluxon_py.tool import import_fluxon_pyo3_local

from .process_runner import bind_current_process_parent_death_sigterm, decode_runtime_config_b64


def run_kv_master_core_service_blocking(*, config_path: Path) -> None:
    fluxon_pyo3 = import_fluxon_pyo3_local()
    result = fluxon_pyo3.run_master_blocking(str(config_path))
    if not result.is_ok():
        raise RuntimeError(f"run_master_blocking failed: {result.unwrap_error()}")

    _ = result.unwrap()


def run_kv_master_core_service_blocking_from_yaml_text(*, config_yaml: str) -> None:
    config = yaml.safe_load(config_yaml)
    if not isinstance(config, dict):
        raise TypeError(f"master config must decode to dict, got {type(config).__name__}")
    fluxon_pyo3 = import_fluxon_pyo3_local()
    result = fluxon_pyo3.run_master_blocking(config)
    if not result.is_ok():
        raise RuntimeError(f"run_master_blocking failed: {result.unwrap_error()}")

    _ = result.unwrap()


def main() -> None:
    bind_current_process_parent_death_sigterm()
    parser = argparse.ArgumentParser(description="Start Fluxon master core service (blocking)")
    parser.add_argument("-c", "--config", type=Path, required=False, help="Path to master YAML config")
    parser.add_argument("-w", "--workdir", type=Path, required=False, help="Working directory")
    parser.add_argument("--config-b64", required=False, help="Base64-encoded YAML config")
    args = parser.parse_args()
    if args.config_b64 is not None:
        if args.workdir is not None:
            resolved_workdir = args.workdir.resolve()
            resolved_workdir.mkdir(parents=True, exist_ok=True)
            os.chdir(resolved_workdir)
        run_kv_master_core_service_blocking_from_yaml_text(
            config_yaml=decode_runtime_config_b64(args.config_b64)
        )
        return
    if args.config is None:
        raise ValueError("--config is required when --config-b64 is not used")
    run_kv_master_core_service_blocking(config_path=args.config)


if __name__ == "__main__":
    main()
