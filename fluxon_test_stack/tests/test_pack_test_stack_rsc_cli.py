#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "pack_test_stack_rsc.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fluxon_test_stack_pack_test_stack_rsc", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_PACK = _load_module()


class TestPackTestStackRscCli(unittest.TestCase):
    def test_resolve_transport_backends_from_ci_suite(self) -> None:
        backends = _PACK._resolve_transport_backends(
            config_path=(REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml").resolve(),
            explicit_profile_ids=[],
        )
        self.assertEqual(backends, ["fastws", "tquic", "sockudo_ws", "tcp"])

    def test_resolve_transport_backends_from_nontransport_profile(self) -> None:
        backends = _PACK._resolve_transport_backends(
            config_path=(REPO_ROOT / "fluxon_test_stack" / "benchmark_full_matrix.yaml").resolve(),
            explicit_profile_ids=["redis_sharded", "alluxio_posix"],
        )
        self.assertEqual(backends, ["fastws"])

    def test_build_plan_reuses_existing_releases(self) -> None:
        release_dir = (REPO_ROOT / ".tmp_test_pack_test_stack_rsc_release").resolve()
        plan = _PACK._build_all_profiles_plan(
            release_dir=release_dir,
            config_path=(REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml").resolve(),
            top_level_transport_backend="fastws",
            rdma_backend="closed_sdk",
            with_tikv_runtime=True,
            transport_backends=["fastws", "tcp"],
            reuse_existing_release=True,
            skip_top_level_release=False,
            repo_test_rsc_root=None,
            prepare_config=None,
            baseline_source_root=None,
            redis_bundle_src=None,
            alluxio_bundle_src=None,
            build_redis_bundle_docker=False,
            redis_version=None,
            redis_source_url=None,
            redis_source_sha256=None,
            redis_docker_image=None,
        )
        self.assertEqual(plan[0]["action"], "validate_release")
        self.assertEqual(plan[0]["scope"], "top_level_release")
        self.assertEqual(plan[1]["action"], "validate_release")
        self.assertEqual(plan[1]["profile_id"], "fluxon_fastws")
        self.assertEqual(plan[2]["action"], "prepare_test_rsc")
        self.assertIn("--out-dir", plan[2]["command"])
        self.assertIn(str((release_dir / "test_rsc" / "fluxon_fastws").resolve()), plan[2]["command"])
        self.assertEqual(plan[3]["profile_id"], "fluxon_tcp")
        self.assertEqual(plan[4]["profile_id"], "fluxon_tcp")

    def test_build_plan_packs_profile_release_under_profiles_dir(self) -> None:
        release_dir = (REPO_ROOT / ".tmp_test_pack_test_stack_rsc_release").resolve()
        plan = _PACK._build_all_profiles_plan(
            release_dir=release_dir,
            config_path=(REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml").resolve(),
            top_level_transport_backend="fastws",
            rdma_backend="closed_sdk",
            with_tikv_runtime=True,
            transport_backends=["fastws"],
            reuse_existing_release=False,
            skip_top_level_release=False,
            repo_test_rsc_root=None,
            prepare_config=None,
            baseline_source_root=None,
            redis_bundle_src=None,
            alluxio_bundle_src=None,
            build_redis_bundle_docker=False,
            redis_version=None,
            redis_source_url=None,
            redis_source_sha256=None,
            redis_docker_image=None,
        )
        self.assertEqual(plan[0]["action"], "pack_release")
        self.assertEqual(plan[0]["release_dir"], str(release_dir))
        self.assertEqual(plan[1]["action"], "pack_release")
        self.assertEqual(plan[1]["release_dir"], str((release_dir / "profiles" / "fluxon_fastws").resolve()))
        self.assertEqual(plan[2]["action"], "prepare_test_rsc")
        self.assertEqual(plan[2]["out_dir"], str((release_dir / "test_rsc" / "fluxon_fastws").resolve()))


if __name__ == "__main__":
    raise SystemExit(unittest.main())
