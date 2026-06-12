#!/usr/bin/env python3
from __future__ import annotations

from _common import run_pytest


TEST_REQUIREMENTS = ["etcd", "kv-cluster", "ops"]


def main() -> int:
    return run_pytest(
        "Flat index entry for heavier MPMC benchmark-style tests.",
        [
            "fluxon_py/tests/test_api_chan_mpmc/test_mpmc_simple_bench.py",
            "fluxon_py/tests/test_api_chan_mpmc/test_mpmc_simple_bench2.py",
        ],
    )


if __name__ == "__main__":
    raise SystemExit(main())
