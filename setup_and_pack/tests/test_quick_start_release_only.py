from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
QUICK_START_BUILD_IMAGE_PATH = REPO_ROOT / "examples" / "fluxon_quick_start" / "build_image.py"
QUICK_START_START_PATH = REPO_ROOT / "examples" / "fluxon_quick_start" / "start.py"


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_BUILD_IMAGE = _load_module("fluxon_quick_start_build_image_test", QUICK_START_BUILD_IMAGE_PATH)
_START = _load_module("fluxon_quick_start_start_test", QUICK_START_START_PATH)


class QuickStartReleaseOnlyTest(unittest.TestCase):
    def test_stage_build_context_copies_release_wheels_without_source_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            release_dir = root / "release"
            context_root = root / "context"
            (release_dir / "ext_images" / "etcd").mkdir(parents=True)
            (release_dir / "ext_images" / "greptime").mkdir(parents=True)
            (release_dir / "ext_images" / "etcd" / "etcd").write_text("etcd", encoding="utf-8")
            (release_dir / "ext_images" / "etcd" / "etcdctl").write_text("etcdctl", encoding="utf-8")
            (release_dir / "ext_images" / "greptime" / "greptime").write_text("greptime", encoding="utf-8")
            (release_dir / "fluxon-0.2.1-py3-none-any.whl").write_text("py", encoding="utf-8")
            (release_dir / "fluxon_pyo3-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl").write_text(
                "pyo3", encoding="utf-8"
            )

            dockerfile_path = _BUILD_IMAGE._stage_build_context(release_dir=release_dir, context_root=context_root)

            self.assertEqual(dockerfile_path, context_root / "examples" / "fluxon_quick_start" / "Dockerfile")
            self.assertTrue((context_root / "fluxon_release" / "fluxon-0.2.1-py3-none-any.whl").is_file())
            self.assertTrue(
                (context_root / "fluxon_release" / "fluxon_pyo3-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl").is_file()
            )
            self.assertFalse((context_root / "fluxon_py").exists())
            self.assertFalse((context_root / "setup.py").exists())

    def test_release_wheel_paths_prefers_release_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            script_dir = root / "examples" / "fluxon_quick_start"
            release_dir = root / "fluxon_release"
            bin_dir = script_dir / "bin"
            script_dir.mkdir(parents=True)
            release_dir.mkdir(parents=True)
            bin_dir.mkdir(parents=True)
            release_wheel = release_dir / "fluxon-0.2.1-py3-none-any.whl"
            bin_wheel = bin_dir / "fluxon_pyo3-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"
            release_wheel.write_text("release", encoding="utf-8")
            bin_wheel.write_text("bin", encoding="utf-8")

            old_script_dir = _START.SCRIPT_DIR
            try:
                _START.SCRIPT_DIR = script_dir
                self.assertEqual(_START._release_wheel_paths(), [release_wheel])
            finally:
                _START.SCRIPT_DIR = old_script_dir

    def test_release_wheel_paths_falls_back_to_bin_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            script_dir = root / "examples" / "fluxon_quick_start"
            bin_dir = script_dir / "bin"
            bin_dir.mkdir(parents=True)
            bin_wheel = bin_dir / "fluxon_pyo3-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"
            bin_wheel.write_text("bin", encoding="utf-8")

            old_script_dir = _START.SCRIPT_DIR
            try:
                _START.SCRIPT_DIR = script_dir
                self.assertEqual(_START._release_wheel_paths(), [bin_wheel])
            finally:
                _START.SCRIPT_DIR = old_script_dir


if __name__ == "__main__":
    unittest.main()
