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


OPS_CONTROLLER_MODULE_NAME = "fluxon_py.runtime.start_ops_controller"
# English note:
# - Self-host ops_controller replacement can take materially longer than stateless services.
# - Keep the larger timeout scoped to ops_controller so unrelated singleton services keep their
#   shorter restart budget.
STOP_EXISTING_OPS_CONTROLLER_TIMEOUT_SECONDS = 180
OPS_CONTROLLER_RUNTIME_CONFIG_FILENAME = "ops_controller.runtime.yaml"


def run_ops_controller_blocking(
    *,
    workdir: Path,
    config: RuntimeConfigInput | None = None,
    config_path: Path | None = None,
) -> None:
    resolved_workdir = workdir.resolve()
    resolved_config = resolve_runtime_config_path(
        workdir=resolved_workdir,
        runtime_config_filename=OPS_CONTROLLER_RUNTIME_CONFIG_FILENAME,
        config=config,
        config_path=config_path,
    )
    singleton_spec = build_runtime_singleton_spec(
        module_name=OPS_CONTROLLER_MODULE_NAME,
        entrypoint_path=Path(__file__),
        workdir=workdir,
    )
    run_singleton_process(
        config_path=resolved_config,
        singleton_spec=singleton_spec,
        stop_timeout_seconds=STOP_EXISTING_OPS_CONTROLLER_TIMEOUT_SECONDS,
        start_fn=lambda: run_ops_controller_service_blocking(
            config_path=resolved_config,
            workdir=resolved_workdir,
        ),
    )


def run_ops_controller_service_blocking(*, config_path: Path, workdir: Path) -> None:
    fluxon_pyo3 = import_fluxon_pyo3_local()
    fluxon_pyo3.fluxon_ops_controller_blocking(str(config_path), str(workdir))


def main() -> None:
    bind_current_process_parent_death_sigterm()
    parser = argparse.ArgumentParser(description="Start Fluxon Ops controller (blocking)")
    parser.add_argument("-c", "--config", type=Path, required=True, help="Path to controller YAML config")
    parser.add_argument("-w", "--workdir", type=Path, required=True, help="Working directory")
    args = parser.parse_args()
    run_ops_controller_blocking(config=args.config, workdir=args.workdir)


if __name__ == "__main__":
    main()
