#!/usr/bin/env python3
from __future__ import annotations

from _common import run_python_file


TEST_REQUIREMENTS = ["etcd", "fluxon-pyo3", "fluxon-release", "greptime", "ops", "submodules"]


def main() -> int:
    return run_python_file(
        "Flat index entry for existing MQ Ctrl-C integration coverage.",
        "fluxon_py/tests/test_mq/test_example_ctrl_c_exit.py",
    )


if __name__ == "__main__":
    raise SystemExit(main())
