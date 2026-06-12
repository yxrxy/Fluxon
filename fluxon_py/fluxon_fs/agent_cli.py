#!/usr/bin/env python3

"""Fluxon FS remote agent.

This CLI starts the FluxonFS agent as a blocking Rust entrypoint.

Contract:
- The handler protocol is user-RPC with FlatDict payloads.
- The Python layer is only an entrypoint wrapper; all core logic is implemented in Rust.
- On mount failures, the process exits with an error (no retry). Retry policy is owned by the caller.

Accepted arguments:
  -c, --config   YAML config file
  -w, --workdir  Workdir for relative paths
"""

from __future__ import annotations


from pathlib import Path

from fluxon_py.runtime.start_fs_agent import run_fs_agent_blocking


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
    import argparse

    parser = argparse.ArgumentParser(description="Fluxon FS remote agent")
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
        help="Workdir; if relative, resolve against this script path",
    )
    args = parser.parse_args()

    config_path = _resolve_script_dir_cli_path(raw_path=args.config, field_name="config")
    workdir = _resolve_script_dir_cli_path(raw_path=args.workdir, field_name="workdir")
    if not config_path.exists():
        raise FileNotFoundError(f"config not found: {config_path}")
    if not workdir.exists():
        raise FileNotFoundError(f"workdir not found: {workdir}")
    run_fs_agent_blocking(config=config_path, workdir=workdir)


if __name__ == "__main__":
    main()
