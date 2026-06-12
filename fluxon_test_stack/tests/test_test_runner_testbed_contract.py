#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = REPO_ROOT / "fluxon_test_stack" / "test_runner.py"


def _load_module():
    runner_dir = RUNNER_PATH.parent
    sys.path.insert(0, str(runner_dir))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_test_stack_test_runner_testbed_contract", RUNNER_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(runner_dir):
            sys.path.pop(0)


_RUNNER = _load_module()


class TestTestRunnerTestbedContract(unittest.TestCase):
    def test_selection_supervisor_authority_comes_from_repo_deployment_codegen(self) -> None:
        _text, script_path = _RUNNER._expected_test_bed_selection_supervisor_text()
        self.assertEqual(script_path, (REPO_ROOT / "deployment" / "gen_bare_deploy_bash.py").resolve())

    def test_bootstrap_runner_uses_repo_start_test_bed_entry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            bundle_root = Path(td)
            start_cfg = bundle_root / "start_test_bed.runner.yaml"
            workdir = bundle_root / "bootstrap_workdir"
            start_cfg.write_text("schema_version: 6\n", encoding="utf-8")
            workdir.mkdir()
            manifest_path = bundle_root / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "start_config_path": str(start_cfg),
                        "workdir": str(workdir),
                        "bootstrap_mode": "apply_only",
                    }
                ),
                encoding="utf-8",
            )

            env = {**os.environ, _RUNNER.TEST_STACK_START_TEST_BED_CONFIG_ENV: str(start_cfg)}
            with mock.patch.dict(os.environ, env, clear=True):
                with mock.patch.object(_RUNNER.subprocess, "run") as run_mock:
                    run_mock.return_value = mock.Mock(returncode=0)
                    ok = _RUNNER._bootstrap_test_bed_via_runner()

            self.assertTrue(ok)
            argv = run_mock.call_args.args[0]
            self.assertEqual(argv[0], sys.executable)
            self.assertEqual(argv[1], str((REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py").resolve()))
            self.assertEqual(
                argv,
                [
                    sys.executable,
                    str((REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py").resolve()),
                    "--config",
                    str(start_cfg),
                    "--workdir",
                    str(workdir),
                    "--bootstrap-mode",
                    "apply_only",
                ],
            )


if __name__ == "__main__":
    raise SystemExit(unittest.main())
