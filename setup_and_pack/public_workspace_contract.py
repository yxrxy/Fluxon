from __future__ import annotations

import shutil
from pathlib import Path


PUBLIC_WORKSPACE_INPUT_RELATIVE_PATHS = (
    "setup.py",
    "fluxon_py",
    "fluxon_release/closed_sdk",
    "fluxon_rs/Cargo.toml",
    "fluxon_rs/Cargo.lock",
    "fluxon_rs/.cargo",
    "fluxon_rs/rust-toolchain.toml",
    "fluxon_rs/fluxon_commu_contract",
    "fluxon_rs/fluxon_commu_closed_sdk_consumer",
    "fluxon_rs/fluxon_commu",
    "fluxon_rs/fluxon_pyo3",
    "fluxon_rs/limit_thirdparty",
    "fluxon_rs/fluxon_kv",
    "fluxon_rs/fluxon_framework",
    "fluxon_rs/fluxon_framework_compiled",
    "fluxon_rs/fluxon_util",
    "fluxon_rs/fluxon_mq",
    "fluxon_rs/fluxon_cli",
    "fluxon_rs/fluxon_ops",
    "fluxon_rs/fluxon_proxy_proto",
    "fluxon_rs/fluxon_proxy",
    "fluxon_rs/fluxon_fs",
    "fluxon_rs/fluxon_fs_core",
    "fluxon_rs/fluxon_fs_s3_gateway",
    "fluxon_rs/fluxon_observability",
    "fluxon_rs/moka",
)


def _copy_public_workspace_input_path(source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if source_path.is_dir():
        shutil.copytree(source_path, target_path, symlinks=True, dirs_exist_ok=True)
        return
    shutil.copy2(source_path, target_path)


def _sanitize_public_workspace_input(*, workspace_root: Path) -> None:
    for pycache_dir in workspace_root.rglob("__pycache__"):
        shutil.rmtree(pycache_dir, ignore_errors=True)
    for pyc_path in workspace_root.rglob("*.pyc"):
        try:
            pyc_path.unlink()
        except FileNotFoundError:
            pass


__all__ = [
    "PUBLIC_WORKSPACE_INPUT_RELATIVE_PATHS",
    "_copy_public_workspace_input_path",
    "_sanitize_public_workspace_input",
]
