#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "gitops" / "gitops_lib.py"


def _load_module():
    module_dir = MODULE_PATH.parent
    sys.path.insert(0, str(module_dir))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_test_stack_gitops_lib_contract", MODULE_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(module_dir):
            sys.path.pop(0)


_GITOPS = _load_module()


class TestGitOpsLibContract(unittest.TestCase):
    def test_load_context_creates_runtime_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_path = root / "gitops.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "interval: 123",
                        "retention:",
                        "  max_age_days: 9",
                        "repos: []",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            ctx = _GITOPS.load_context(config_path=config_path, workdir=root / "runtime")
            desc = _GITOPS.describe_context(ctx)
            self.assertEqual(desc["interval"], 123)
            self.assertEqual(desc["max_age_days"], 9)
            self.assertEqual(desc["repo_count"], 0)
            self.assertTrue(ctx.log_dir.is_dir())
            self.assertTrue(ctx.run_meta_dir.is_dir())
            self.assertTrue(ctx.repos_dir.is_dir())
            self.assertTrue(ctx.runs_dir.is_dir())

    def test_read_log_chunk_supports_run_and_step_logs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_path = root / "gitops.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "interval: 60",
                        "retention:",
                        "  max_age_days: 7",
                        "repos:",
                        "  - addr: repo",
                        "    follow:",
                        "      - branch: main",
                        "        run:",
                        "          name_prefix: ci",
                        "          commands:",
                        "            - echo one",
                        "            - echo two",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            ctx = _GITOPS.load_context(config_path=config_path, workdir=root / "runtime")
            run_id = "repo__main__20260608_120000__abcdef0"
            meta_dir = ctx.run_meta_dir / run_id
            steps_dir = meta_dir / "steps"
            steps_dir.mkdir(parents=True)
            run_log = meta_dir / "run.log"
            step_log = steps_dir / "step_1.log"
            progress_file = meta_dir / "progress.jsonl"
            result_file = meta_dir / "result.yaml"
            run_log.write_text("0123456789", encoding="utf-8")
            step_log.write_text("step-one-log", encoding="utf-8")
            progress_file.write_text(
                json.dumps({"event": "step_started", "payload": {"idx": 1}, "ts": 1.0}) + "\n",
                encoding="utf-8",
            )
            result_file.write_text("ok: true\n", encoding="utf-8")
            _GITOPS._dump_yaml_atomic(  # type: ignore[attr-defined]
                ctx.run_index_file,
                {
                    "runs": {
                        run_id: {
                            "repo": "repo",
                            "branch": "main",
                            "commit": "abcdef0123456789",
                            "name_prefix": "ci",
                            "status": "ok",
                            "started_ts": "2026-06-08T12:00:00",
                            "finished_ts": "2026-06-08T12:01:00",
                            "log_file": str(run_log),
                            "progress_file": str(progress_file),
                            "result_file": str(result_file),
                            "run_dir": str(ctx.runs_dir / "sample_run_dir"),
                            "meta_dir": str(meta_dir),
                        }
                    }
                },
            )
            tail = _GITOPS.read_log_chunk(
                ctx,
                run_id=run_id,
                kind="run",
                step=None,
                from_offset=None,
                before_offset=None,
                max_bytes=4,
            )
            self.assertEqual(tail["text"], "6789")
            older = _GITOPS.read_log_chunk(
                ctx,
                run_id=run_id,
                kind="run",
                step=None,
                from_offset=None,
                before_offset=6,
                max_bytes=4,
            )
            self.assertEqual(older["text"], "2345")
            step_chunk = _GITOPS.read_log_chunk(
                ctx,
                run_id=run_id,
                kind="step",
                step=1,
                from_offset=5,
                before_offset=None,
                max_bytes=4,
            )
            self.assertEqual(step_chunk["text"], "one-")
            self.assertEqual(_GITOPS.get_run_step_count(ctx, run_id), 2)
            self.assertEqual(_GITOPS.get_run_last_event(ctx, run_id)["event"], "step_started")

    def test_rerun_target_rejects_unknown_repo_branch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_path = root / "gitops.yaml"
            config_path.write_text("repos: []\n", encoding="utf-8")
            ctx = _GITOPS.load_context(config_path=config_path, workdir=root / "runtime")
            with self.assertRaisesRegex(ValueError, "unknown repo/branch"):
                _GITOPS.rerun_target(ctx, target="repo:main:abcdef")


if __name__ == "__main__":
    raise SystemExit(unittest.main())
