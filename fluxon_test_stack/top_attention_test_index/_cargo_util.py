#!/usr/bin/env python3
from __future__ import annotations

from _common import REPO_ROOT, run_cargo


TEST_REQUIREMENTS = ["cargo", "etcd", "ops", "submodules"]


def main() -> int:
    return run_cargo([
        "test",
        "--manifest-path",
        str(REPO_ROOT / "fluxon_rs" / "fluxon_util" / "Cargo.toml"),
    ])


if __name__ == "__main__":
    raise SystemExit(main())
