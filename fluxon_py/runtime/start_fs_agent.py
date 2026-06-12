#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from fluxon_py.logging import init_logger
from fluxon_py.tool import import_fluxon_pyo3_local

from .process_runner import (
    bind_current_process_parent_death_sigterm,
    build_runtime_singleton_spec,
    RuntimeConfigInput,
    decode_runtime_config_b64,
    encode_runtime_config_b64,
    resolve_runtime_config_path,
    run_singleton_process,
    start_python_module_process,
    start_python_module_process_with_config_b64,
)


FS_AGENT_MODULE_NAME = "fluxon_py.runtime.start_fs_agent"
STOP_EXISTING_FS_AGENT_TIMEOUT_SECONDS = 30
FS_AGENT_RUNTIME_CONFIG_FILENAME = "fs_agent.runtime.yaml"


def run_fs_agent_blocking(
    *,
    workdir: Path,
    config: RuntimeConfigInput | None = None,
    config_path: Path | None = None,
) -> None:
    resolved_workdir = workdir.resolve()
    resolved_config = resolve_runtime_config_path(
        workdir=resolved_workdir,
        runtime_config_filename=FS_AGENT_RUNTIME_CONFIG_FILENAME,
        config=config,
        config_path=config_path,
    )
    singleton_spec = build_runtime_singleton_spec(
        module_name=FS_AGENT_MODULE_NAME,
        entrypoint_path=Path(__file__),
        workdir=workdir,
    )
    run_singleton_process(
        config_path=resolved_config,
        singleton_spec=singleton_spec,
        stop_timeout_seconds=STOP_EXISTING_FS_AGENT_TIMEOUT_SECONDS,
        start_fn=lambda: run_fs_agent_service_blocking(
            config_path=resolved_config,
            workdir=resolved_workdir,
        ),
    )


def run_fs_agent_service_blocking(*, config_path: Path, workdir: Path) -> None:
    log = init_logger("fluxon_fs_agent")
    log.info("Starting fluxon_fs agent (Rust): config=%s workdir=%s", config_path, workdir)
    fluxon_pyo3 = import_fluxon_pyo3_local()
    fluxon_pyo3.fluxon_fs_agent_blocking(str(config_path), str(workdir))


def run_fs_agent_service_blocking_from_yaml_text(*, config_yaml: str) -> None:
    log = init_logger("fluxon_fs_agent")
    log.info("Starting fluxon_fs agent (Rust): config transport=config_b64")
    fluxon_pyo3 = import_fluxon_pyo3_local()
    fluxon_pyo3.fluxon_fs_agent_blocking_from_yaml_text(config_yaml)


def start_fs_agent_process(
    *,
    workdir: Path | None = None,
    config: RuntimeConfigInput | None = None,
    config_path: Path | None = None,
    log_path: Path | None = None,
) -> subprocess.Popen[bytes]:
    if config_path is None and isinstance(config, dict) and workdir is None:
        return start_fs_agent_process_with_config_b64(config=config, log_path=log_path)
    if workdir is None:
        raise ValueError("workdir is required when config is not a dict and config_path is not provided")
    resolved_workdir = workdir.resolve()
    resolved_config = resolve_runtime_config_path(
        workdir=resolved_workdir,
        runtime_config_filename=FS_AGENT_RUNTIME_CONFIG_FILENAME,
        config=config,
        config_path=config_path,
    )
    return start_python_module_process(
        module_name=FS_AGENT_MODULE_NAME,
        config_path=resolved_config,
        workdir=resolved_workdir,
        extra_cli_args=(),
        log_path=log_path,
    )


def start_fs_agent_process_with_config_b64(
    *,
    config: dict,
    log_path: Path | None = None,
) -> subprocess.Popen[bytes]:
    return start_python_module_process_with_config_b64(
        module_name=FS_AGENT_MODULE_NAME,
        config_b64=encode_runtime_config_b64(config),
        extra_cli_args=(),
        log_path=log_path,
    )


def main() -> None:
    bind_current_process_parent_death_sigterm()
    parser = argparse.ArgumentParser(description="Start Fluxon FS remote agent (blocking)")
    parser.add_argument("-c", "--config", type=Path, required=False, help="Path to agent YAML config")
    parser.add_argument("-w", "--workdir", type=Path, required=False, help="Working directory")
    parser.add_argument("--config-b64", required=False, help="Base64-encoded YAML config")
    args = parser.parse_args()
    if args.config_b64 is not None:
        run_fs_agent_service_blocking_from_yaml_text(
            config_yaml=decode_runtime_config_b64(args.config_b64)
        )
        return
    if args.config is None or args.workdir is None:
        raise ValueError("--config and --workdir are required when --config-b64 is not used")
    run_fs_agent_blocking(config=args.config, workdir=args.workdir)


if __name__ == "__main__":
    main()
