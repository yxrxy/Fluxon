#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock
import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "ops_ci.py"
CI_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "ci.yml"


def _load_module():
    spec = importlib.util.spec_from_file_location("fluxon_ops_ci_pipeline_contract", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_OPS_CI = _load_module()


class TestOpsCiPipelineContract(unittest.TestCase):
    def test_build_release_uses_repo_pack_release_entry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            release_dir = Path(td) / "release"
            with mock.patch.object(_OPS_CI, "_run_checked", return_value=0) as run_mock:
                rc = _OPS_CI.build_release(
                    release_dir=release_dir,
                    transport_backend="tcp_thread",
                    rdma_backend="closed_sdk",
                    with_tikv_runtime=True,
                )

        self.assertEqual(rc, 0)
        argv = run_mock.call_args.args[0]
        self.assertEqual(
            list(argv),
            [
                sys.executable,
                str((REPO_ROOT / "setup_and_pack" / "pack_release.py").resolve()),
                "--transport-backend",
                "tcp_thread",
                "--rdma-backend",
                "closed_sdk",
                "--with-tikv-runtime",
                "true",
                "--release-dir",
                str(release_dir.resolve()),
            ],
        )

    def test_start_test_bed_uses_repo_bootstrap_entry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config_path = Path(td) / "start_test_bed.yaml"
            workdir = Path(td) / "workdir"
            config_path.write_text("schema_version: 6\n", encoding="utf-8")
            workdir.mkdir()
            with mock.patch.object(_OPS_CI, "_run_checked", return_value=0) as run_mock:
                rc = _OPS_CI.start_test_bed(
                    config_path=config_path,
                    workdir=workdir,
                    bootstrap_mode="apply_only",
                )

        self.assertEqual(rc, 0)
        argv = run_mock.call_args.args[0]
        self.assertEqual(
            list(argv),
            [
                sys.executable,
                str((REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py").resolve()),
                "--config",
                str(config_path.resolve()),
                "--workdir",
                str(workdir.resolve()),
                "--bootstrap-mode",
                "apply_only",
            ],
        )

    def test_build_doc_site_sets_explicit_base_url(self) -> None:
        captured_env: dict[str, str] | None = None

        def _capture(argv, *, env=None):
            nonlocal captured_env
            captured_env = env
            return 0

        with mock.patch.object(_OPS_CI, "_run_checked", side_effect=_capture) as run_mock:
            rc = _OPS_CI.build_doc_site(
                base_url="example.com/project",
                base_url_from_github_env=False,
            )

        self.assertEqual(rc, 0)
        argv = run_mock.call_args.args[0]
        self.assertEqual(
            list(argv),
            [
                sys.executable,
                str((REPO_ROOT / "scripts" / "build_doc_site.py").resolve()),
                "build",
            ],
        )
        assert captured_env is not None
        self.assertEqual(captured_env["FLUXON_DOC_SITE_BASE_URL"], "example.com/project")

    def test_build_doc_site_can_derive_base_url_from_github_env(self) -> None:
        env = {
            **os.environ,
            "GITHUB_REPOSITORY_OWNER": "Tele-AI",
            "GITHUB_REPOSITORY": "Tele-AI/Fluxon-action",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            captured_env: dict[str, str] | None = None

            def _capture(argv, *, env=None):
                nonlocal captured_env
                captured_env = env
                return 0

            with mock.patch.object(_OPS_CI, "_run_checked", side_effect=_capture):
                rc = _OPS_CI.build_doc_site(
                    base_url=None,
                    base_url_from_github_env=True,
                )

        self.assertEqual(rc, 0)
        assert captured_env is not None
        self.assertEqual(captured_env["FLUXON_DOC_SITE_BASE_URL"], "tele-ai.github.io/fluxon-action")

    def test_workflow_contract_tests_uses_curated_default_modules(self) -> None:
        with mock.patch.object(_OPS_CI, "_run_checked", return_value=0) as run_mock:
            rc = _OPS_CI.workflow_contract_tests(test_modules=())

        self.assertEqual(rc, 0)
        argv = run_mock.call_args.args[0]
        self.assertEqual(
            list(argv),
            [
                sys.executable,
                "-m",
                "unittest",
                "deployment.tests.test_ops_ci_pipeline_contract",
                "deployment.tests.test_build_doc_site_contract",
                "fluxon_test_stack.tests.test_test_runner_testbed_contract",
            ],
        )

    def test_ci_workflow_builds_doc_site_via_ops_ci_entry(self) -> None:
        workflow = yaml.safe_load(CI_WORKFLOW_PATH.read_text(encoding="utf-8"))
        self.assertIsInstance(workflow, dict)
        self.assertEqual(workflow.get("name"), "CI")
        jobs = workflow.get("jobs")
        self.assertIsInstance(jobs, dict)
        job = jobs.get("build-doc-site")
        self.assertIsInstance(job, dict)
        self.assertEqual(job.get("name"), "Build doc site")
        steps = job.get("steps")
        self.assertIsInstance(steps, list)
        run_steps = [step for step in steps if isinstance(step, dict) and step.get("name") == "Build doc site"]
        self.assertEqual(len(run_steps), 1)
        self.assertEqual(
            run_steps[0].get("run"),
            "python3 fluxon_test_stack/ops_ci.py build-doc-site --base-url-from-github-env",
        )


if __name__ == "__main__":
    unittest.main()
