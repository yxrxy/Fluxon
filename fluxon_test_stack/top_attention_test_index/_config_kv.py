#!/usr/bin/env python3
from __future__ import annotations

from _common import run_python_file


TEST_REQUIREMENTS = ["ops"]


def main() -> int:
    return run_python_file(
        "Flat index entry for KV Python config/schema tests.",
        "fluxon_py/tests/test_config.py",
    )


if __name__ == "__main__":
    raise SystemExit(main())
