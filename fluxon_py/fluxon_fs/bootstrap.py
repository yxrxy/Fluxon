from __future__ import annotations

from pathlib import Path
from typing import Any

from .patcher import FluxonFsPatcher


def install_patcher_from_master(*, kv_store: Any, config_path: Path) -> FluxonFsPatcher:
    """Install global FS interception and start config fetch from master.

    Notes:
    - Control plane (User-RPC + retry until success) is implemented in Rust.
    - This call starts a background Rust thread; use `patcher.wait_cache_config_loaded()`
      if the caller needs to block until config is ready.
    """
    patcher = FluxonFsPatcher(kv_store)
    patcher.start_cache_config_fetch_from_master_config_file(config_path)
    patcher.install()
    return patcher
