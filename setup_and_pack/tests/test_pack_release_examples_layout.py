from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PACK_RELEASE_PATH = REPO_ROOT / "setup_and_pack" / "pack_release.py"


def _load_module():
    for import_root in (
        REPO_ROOT,
        PACK_RELEASE_PATH.parent,
        PACK_RELEASE_PATH.parent.parent,
    ):
        import_root_str = str(import_root)
        if import_root_str in sys.path:
            sys.path.remove(import_root_str)
        sys.path.insert(0, import_root_str)
    spec = importlib.util.spec_from_file_location("setup_and_pack_pack_release_examples_test", PACK_RELEASE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_PACK_RELEASE = _load_module()


class PackReleaseExamplesLayoutTest(unittest.TestCase):
    def test_resolve_examples_dir_prefers_app_examples(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            (repo_root / "app" / "examples").mkdir(parents=True)
            (repo_root / "examples").mkdir()

            resolved = _PACK_RELEASE._resolve_examples_dir(repo_root=repo_root)

            self.assertEqual(resolved, repo_root / "app" / "examples")

    def test_resolve_examples_dir_falls_back_to_examples(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            (repo_root / "examples").mkdir(parents=True)

            resolved = _PACK_RELEASE._resolve_examples_dir(repo_root=repo_root)

            self.assertEqual(resolved, repo_root / "examples")


if __name__ == "__main__":
    unittest.main()
