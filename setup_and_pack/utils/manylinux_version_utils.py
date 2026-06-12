from __future__ import annotations

from pathlib import Path

import yaml

__all__ = [
    "SUPPORTED_MANYLINUX_VERSIONS",
    "load_manylinux_version_static",
]

# Single source of truth for manylinux selection across build scripts.
# Keep this list aligned with the supported build images and wheel naming.
SUPPORTED_MANYLINUX_VERSIONS: tuple[str, ...] = ("2_28",)


def load_manylinux_version_static(*, repo_root: Path) -> str:
    """Read and validate manylinux_version from build_config_ext_static.yml."""
    config_file = (repo_root / "build_config_ext_static.yml").resolve()
    if not config_file.exists():
        raise FileNotFoundError(f"Missing static build config file: {config_file}")

    cfg = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    if cfg is None:
        raise ValueError(f"Static build config is empty: {config_file}")
    if not isinstance(cfg, dict):
        raise ValueError(f"Static build config must be a YAML mapping: {config_file}")

    manylinux_version = cfg.get("manylinux_version")
    if not isinstance(manylinux_version, str) or not manylinux_version.strip():
        raise ValueError(
            f"Missing required field in {config_file}: manylinux_version (e.g. \"2_28\")"
        )

    manylinux_version = manylinux_version.strip().strip("'\"")
    if manylinux_version not in SUPPORTED_MANYLINUX_VERSIONS:
        raise ValueError(
            f"Config error: manylinux_version={manylinux_version} not supported; "
            f"supported={SUPPORTED_MANYLINUX_VERSIONS}"
        )

    return manylinux_version
