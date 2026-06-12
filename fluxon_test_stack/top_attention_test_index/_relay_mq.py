#!/usr/bin/env python3
from __future__ import annotations

from _common import run_pytest


TEST_REQUIREMENTS = ["docker", "etcd", "fluxon-release", "ops", "submodules"]


def main() -> int:
    return run_pytest(
        "Flat index entry for MQ relay docker coverage.",
        ["fluxon_py/tests/test_backend_relay_docker.py"],
    )


if __name__ == "__main__":
    raise SystemExit(main())
