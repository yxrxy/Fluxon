#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from fluxon_py.tool import import_fluxon_pyo3_local

from .process_runner import (
    bind_current_process_parent_death_sigterm,
    build_runtime_singleton_spec,
    RuntimeConfigInput,
    resolve_runtime_config_path,
    run_singleton_process,
)


OPS_AGENT_MODULE_NAME = "fluxon_py.runtime.start_ops_agent"
STOP_EXISTING_OPS_AGENT_TIMEOUT_SECONDS = 30
OPS_AGENT_RUNTIME_CONFIG_FILENAME = "ops_agent.runtime.yaml"


def run_ops_agent_blocking(
    *,
    workdir: Path,
    config: RuntimeConfigInput | None = None,
    config_path: Path | None = None,
) -> None:
    resolved_workdir = workdir.resolve()
    resolved_config = resolve_runtime_config_path(
        workdir=resolved_workdir,
        runtime_config_filename=OPS_AGENT_RUNTIME_CONFIG_FILENAME,
        config=config,
        config_path=config_path,
    )
    singleton_spec = build_runtime_singleton_spec(
        module_name=OPS_AGENT_MODULE_NAME,
        entrypoint_path=Path(__file__),
        workdir=workdir,
    )
    run_singleton_process(
        config_path=resolved_config,
        singleton_spec=singleton_spec,
        stop_timeout_seconds=STOP_EXISTING_OPS_AGENT_TIMEOUT_SECONDS,
        start_fn=lambda: run_ops_agent_service_blocking(
            config_path=resolved_config,
            workdir=resolved_workdir,
        ),
    )


def run_ops_agent_service_blocking(*, config_path: Path, workdir: Path) -> None:
    fluxon_pyo3 = import_fluxon_pyo3_local()
    fluxon_pyo3.fluxon_ops_agent_blocking(str(config_path), str(workdir))


def main() -> None:
    bind_current_process_parent_death_sigterm()
    parser = argparse.ArgumentParser(description="Start Fluxon Ops agent (blocking)")
    parser.add_argument("-c", "--config", type=Path, required=True, help="Path to agent YAML config")
    parser.add_argument("-w", "--workdir", type=Path, required=True, help="Working directory")
    args = parser.parse_args()
    run_ops_agent_blocking(config=args.config, workdir=args.workdir)


if __name__ == "__main__":
    main()
