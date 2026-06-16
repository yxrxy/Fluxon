#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "build_doc_site.py"
DOCS_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "docs-pages.yml"


def _load_module():
    spec = importlib.util.spec_from_file_location("fluxon_build_doc_site_contract", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_DOC_SITE = _load_module()


class TestBuildDocSiteContract(unittest.TestCase):
    def test_counterpart_routes_cover_home_and_doc_pairs(self) -> None:
        routes = _DOC_SITE.LANGUAGE_COUNTERPART_ROUTES
        self.assertEqual(routes["/"], "/cn")
        self.assertEqual(routes["/cn"], "/")
        self.assertEqual(
            routes["/user_doc/User---0---Installation"],
            "/cn/user_doc/用户---0---安装",
        )
        self.assertEqual(
            routes["/cn/dev_doc/开发者---2---打包中间件和镜像"],
            "/dev_doc/Developer---2---Package-Middleware-and-Images",
        )

    def test_rewrite_homepage_target_path_for_english_home(self) -> None:
        self.assertEqual(
            _DOC_SITE.rewrite_homepage_target_path("./README_CN.md", language="en"),
            "./cn/",
        )
        self.assertEqual(
            _DOC_SITE.rewrite_homepage_target_path("./fluxon_doc_en/user_doc/", language="en"),
            "./user_doc/",
        )
        self.assertEqual(
            _DOC_SITE.rewrite_homepage_target_path("./fluxon_doc_cn/user_doc/", language="en"),
            "./cn/user_doc/",
        )

    def test_rewrite_homepage_target_path_for_chinese_home(self) -> None:
        self.assertEqual(
            _DOC_SITE.rewrite_homepage_target_path("./README.md", language="cn"),
            "../",
        )
        self.assertEqual(
            _DOC_SITE.rewrite_homepage_target_path("./fluxon_doc_cn/user_doc/", language="cn"),
            "../cn/user_doc/",
        )
        self.assertEqual(
            _DOC_SITE.rewrite_homepage_target_path("./fluxon_rs/rust-toolchain.toml", language="cn"),
            "../fluxon_rs/rust-toolchain.toml",
        )

    def test_docs_pages_workflow_calls_ops_ci_doc_site_entry(self) -> None:
        workflow = yaml.safe_load(DOCS_WORKFLOW_PATH.read_text(encoding="utf-8"))
        self.assertIsInstance(workflow, dict)
        jobs = workflow.get("jobs")
        self.assertIsInstance(jobs, dict)
        build_job = jobs.get("build")
        self.assertIsInstance(build_job, dict)
        steps = build_job.get("steps")
        self.assertIsInstance(steps, list)
        build_steps = [step for step in steps if isinstance(step, dict) and step.get("name") == "Build doc site"]
        self.assertEqual(len(build_steps), 1)
        self.assertEqual(
            build_steps[0].get("run"),
            "python3 fluxon_test_stack/ops_ci.py build-doc-site --base-url-from-github-env",
        )


if __name__ == "__main__":
    unittest.main()
