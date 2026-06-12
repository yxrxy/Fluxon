#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path
import yaml

from fluxon_py.tool import import_fluxon_pyo3_local

from .process_runner import (
    bind_current_process_parent_death_sigterm,
    decode_runtime_config_b64,
    resolve_runtime_config_path,
)


MONITOR_RUNTIME_CONFIG_FILENAME = "monitor_web.runtime.yaml"


def run_monitor_web_service_blocking(*, config_path: Path, workdir: Path) -> None:
    fluxon_pyo3 = import_fluxon_pyo3_local()
    fluxon_pyo3.monitor_http_blocking(str(config_path), str(workdir))


def run_monitor_web_service_blocking_from_yaml_text(*, config_yaml: str, workdir: Path) -> None:
    config = yaml.safe_load(config_yaml)
    if not isinstance(config, dict):
        raise TypeError(f"monitor config must decode to dict, got {type(config).__name__}")
    resolved_workdir = workdir.resolve()
    resolved_config = resolve_runtime_config_path(
        workdir=resolved_workdir,
        runtime_config_filename=MONITOR_RUNTIME_CONFIG_FILENAME,
        config=config,
    )
    run_monitor_web_service_blocking(config_path=resolved_config, workdir=resolved_workdir)


def main() -> None:
    bind_current_process_parent_death_sigterm()
    parser = argparse.ArgumentParser(description="Start Fluxon monitor web service (blocking)")
    parser.add_argument("-c", "--config", type=Path, required=False, help="Path to monitor YAML config")
    parser.add_argument("-w", "--workdir", type=Path, required=True, help="Working directory")
    parser.add_argument("--config-b64", required=False, help="Base64-encoded YAML config")
    args = parser.parse_args()
    if args.config_b64 is not None:
        run_monitor_web_service_blocking_from_yaml_text(
            config_yaml=decode_runtime_config_b64(args.config_b64),
            workdir=args.workdir,
        )
        return
    if args.config is None:
        raise ValueError("--config is required when --config-b64 is not used")
    run_monitor_web_service_blocking(config_path=args.config, workdir=args.workdir)


if __name__ == "__main__":
    main()
