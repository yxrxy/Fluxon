#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
PACK_RELEASE_PATH = REPO_ROOT / "setup_and_pack" / "pack_release.py"
START_TEST_BED_PATH = REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py"
BUILD_DOC_SITE_PATH = REPO_ROOT / "scripts" / "build_doc_site.py"
DEFAULT_WORKFLOW_TEST_MODULES: tuple[str, ...] = (
    "deployment.tests.test_ops_ci_pipeline_contract",
    "deployment.tests.test_build_doc_site_contract",
    "fluxon_test_stack.tests.test_test_runner_testbed_contract",
)
BOOTSTRAP_MODE_CHOICES: tuple[str, ...] = (
    "bare_then_apply",
    "apply_only",
    "bare_only",
)


def _resolve_repo_root_cli_path(*, raw_path: Path, field_name: str) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()
    resolved = (REPO_ROOT / raw_path).resolve()
    if not resolved:
        raise RuntimeError(f"failed to resolve {field_name} against repo root: raw={raw_path}")
    return resolved


def _run_checked(argv: Sequence[str], *, env: dict[str, str] | None = None) -> int:
    subprocess.check_call(list(argv), cwd=str(REPO_ROOT), env=env)
    return 0


def build_release(
    *,
    release_dir: Path | None,
    transport_backend: str,
    rdma_backend: str,
    with_tikv_runtime: bool,
) -> int:
    argv = [
        sys.executable,
        str(PACK_RELEASE_PATH),
        "--transport-backend",
        transport_backend,
        "--rdma-backend",
        rdma_backend,
        "--with-tikv-runtime",
        "true" if with_tikv_runtime else "false",
    ]
    if release_dir is not None:
        argv.extend(
            [
                "--release-dir",
                str(_resolve_repo_root_cli_path(raw_path=release_dir, field_name="release-dir")),
            ]
        )
    return _run_checked(argv)


def start_test_bed(
    *,
    config_path: Path,
    workdir: Path,
    bootstrap_mode: str,
) -> int:
    if bootstrap_mode not in BOOTSTRAP_MODE_CHOICES:
        raise ValueError(
            f"unsupported bootstrap_mode={bootstrap_mode!r}; allowed={list(BOOTSTRAP_MODE_CHOICES)}"
        )
    argv = [
        sys.executable,
        str(START_TEST_BED_PATH),
        "--config",
        str(_resolve_repo_root_cli_path(raw_path=config_path, field_name="config")),
        "--workdir",
        str(_resolve_repo_root_cli_path(raw_path=workdir, field_name="workdir")),
        "--bootstrap-mode",
        bootstrap_mode,
    ]
    return _run_checked(argv)


def _github_pages_base_url_from_env(*, env: dict[str, str]) -> str:
    owner = str(env.get("GITHUB_REPOSITORY_OWNER", "")).strip().lower()
    repo = str(env.get("GITHUB_REPOSITORY", "")).strip()
    if not owner or not repo or "/" not in repo:
        raise ValueError(
            "GITHUB_REPOSITORY_OWNER and GITHUB_REPOSITORY must be set when using --base-url-from-github-env"
        )
    repo_name = repo.split("/", 1)[1].strip()
    if not repo_name:
        raise ValueError("GITHUB_REPOSITORY must include a non-empty repo name")
    repo_name = repo_name.lower()
    if repo_name.endswith(".github.io"):
        return repo_name
    return f"{owner}.github.io/{repo_name}"


def build_doc_site(
    *,
    base_url: str | None,
    base_url_from_github_env: bool,
) -> int:
    env = dict(os.environ)
    if base_url_from_github_env:
        env["FLUXON_DOC_SITE_BASE_URL"] = _github_pages_base_url_from_env(env=env)
    elif base_url is not None:
        env["FLUXON_DOC_SITE_BASE_URL"] = str(base_url).strip()
    argv = [sys.executable, str(BUILD_DOC_SITE_PATH), "build"]
    return _run_checked(argv, env=env)


def workflow_contract_tests(*, test_modules: Sequence[str]) -> int:
    modules = list(test_modules) if test_modules else list(DEFAULT_WORKFLOW_TEST_MODULES)
    argv = [sys.executable, "-m", "unittest", *modules]
    return _run_checked(argv)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fluxon Ops CI/workflow entrypoints.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_release_parser = subparsers.add_parser(
        "build-release",
        help="Build release artifacts through the repo-owned release entrypoint.",
    )
    build_release_parser.add_argument(
        "--release-dir",
        type=Path,
        default=None,
        help="Release directory; if relative, resolve against the repo root.",
    )
    build_release_parser.add_argument(
        "--transport-backend",
        default="tcp_thread",
        help="Transport backend passed to setup_and_pack/pack_release.py.",
    )
    build_release_parser.add_argument(
        "--rdma-backend",
        default="closed_sdk",
        help="RDMA backend passed to setup_and_pack/pack_release.py.",
    )
    build_release_parser.add_argument(
        "--with-tikv-runtime",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Whether release ext_images should include TiKV runtime binaries.",
    )

    start_test_bed_parser = subparsers.add_parser(
        "start-testbed",
        help="Start the shared test bed through the repo-owned bootstrap entrypoint.",
    )
    start_test_bed_parser.add_argument("--config", type=Path, required=True)
    start_test_bed_parser.add_argument("--workdir", type=Path, required=True)
    start_test_bed_parser.add_argument(
        "--bootstrap-mode",
        choices=list(BOOTSTRAP_MODE_CHOICES),
        default="bare_then_apply",
    )

    build_doc_site_parser = subparsers.add_parser(
        "build-doc-site",
        help="Build the Quartz doc site through the repo-owned doc-site entrypoint.",
    )
    build_doc_site_parser.add_argument(
        "--base-url",
        default=None,
        help="Optional FLUXON_DOC_SITE_BASE_URL value.",
    )
    build_doc_site_parser.add_argument(
        "--base-url-from-github-env",
        action="store_true",
        help="Derive FLUXON_DOC_SITE_BASE_URL from GITHUB_REPOSITORY_OWNER/GITHUB_REPOSITORY.",
    )

    contract_tests_parser = subparsers.add_parser(
        "workflow-contract-tests",
        help="Run curated workflow contract tests for release/testbed/doc-site flows.",
    )
    contract_tests_parser.add_argument(
        "--test-module",
        dest="test_modules",
        action="append",
        default=[],
        help="Extra unittest module to run. Defaults to the curated workflow contract set.",
    )

    args = parser.parse_args()
    if args.command == "build-release":
        return build_release(
            release_dir=args.release_dir,
            transport_backend=str(args.transport_backend),
            rdma_backend=str(args.rdma_backend),
            with_tikv_runtime=bool(args.with_tikv_runtime),
        )
    if args.command == "start-testbed":
        return start_test_bed(
            config_path=args.config,
            workdir=args.workdir,
            bootstrap_mode=str(args.bootstrap_mode),
        )
    if args.command == "build-doc-site":
        return build_doc_site(
            base_url=None if args.base_url is None else str(args.base_url),
            base_url_from_github_env=bool(args.base_url_from_github_env),
        )
    if args.command == "workflow-contract-tests":
        return workflow_contract_tests(test_modules=tuple(args.test_modules))
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
