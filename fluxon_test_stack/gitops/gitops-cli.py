#!/usr/bin/env python3
"""Deprecated standalone GitOps CLI entry."""

from __future__ import annotations

def main() -> None:
    raise SystemExit(
        "gitops standalone cli is removed; start fluxon_test_stack/test_runner_ui.py "
        "--gitops-config <gitops.yaml> and use /api/gitops/rerun"
    )


if __name__ == "__main__":
    main()
