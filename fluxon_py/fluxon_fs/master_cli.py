#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from fluxon_py.runtime.start_fs_master import (
    run_fs_master_blocking,
    run_fs_transfer_check_blocking,
)

BASE_DIR = Path(__file__).resolve().parent


def _resolve_script_dir_cli_path(*, raw_path: str, field_name: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path.resolve()
    resolved = (BASE_DIR / path).resolve()
    if not resolved:
        raise RuntimeError(f"failed to resolve {field_name} against script dir: raw={raw_path}")
    return resolved


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fluxon FS master (single-process). Python is only an entrypoint; "
            "all HTTP handlers and S3 gateway are implemented in Rust."
        )
    )
    parser.add_argument(
        "--transfer-check-only",
        action="store_true",
        help="Run only the transfer scheduler control plane without starting the HTTP panel",
    )
    parser.add_argument(
        "--config",
        "-c",
        required=True,
        help="YAML config file; if relative, resolve against this script path",
    )
    parser.add_argument(
        "--workdir",
        "-w",
        required=True,
        help="Workdir for relative paths; if relative, resolve against this script path",
    )
    args = parser.parse_args()

    config_path = _resolve_script_dir_cli_path(raw_path=args.config, field_name="config")
    workdir = _resolve_script_dir_cli_path(raw_path=args.workdir, field_name="workdir")
    if not config_path.exists():
        raise FileNotFoundError(f"config not found: {config_path}")
    if not workdir.exists():
        raise FileNotFoundError(f"workdir not found: {workdir}")
    if args.transfer_check_only:
        run_fs_transfer_check_blocking(config=config_path, workdir=workdir)
        return
    run_fs_master_blocking(config=config_path, workdir=workdir)


if __name__ == "__main__":
    main()
