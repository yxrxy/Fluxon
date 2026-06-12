from __future__ import annotations

import unittest
from pathlib import Path

from requirements_all import (
    ALL_TEST_REQUIREMENTS,
    ALL_TEST_REQUIREMENTS_SET,
    extract_test_requirements,
    iter_test_python_paths,
)


TEST_REQUIREMENTS: list[str] = ["ops"]


class TestTestRequirements(unittest.TestCase):
    def test_every_test_py_declares_requirements(self) -> None:
        for path in iter_test_python_paths():
            with self.subTest(path=path.name):
                _ = extract_test_requirements(path)

    def test_declared_requirement_lists_are_sorted_unique_and_known(self) -> None:
        for path in iter_test_python_paths():
            requirements = extract_test_requirements(path)
            with self.subTest(path=path.name):
                self.assertEqual(requirements, sorted(set(requirements)))
                self.assertTrue(set(requirements).issubset(ALL_TEST_REQUIREMENTS_SET))

    def test_requirement_universe_is_sorted_unique(self) -> None:
        self.assertEqual(list(ALL_TEST_REQUIREMENTS), sorted(set(ALL_TEST_REQUIREMENTS)))

    def test_requirement_universe_matches_declared_union(self) -> None:
        declared_union: set[str] = set()
        for path in iter_test_python_paths():
            declared_union.update(extract_test_requirements(path))
        self.assertEqual(declared_union, set(ALL_TEST_REQUIREMENTS))

    def test_removed_direct_entrypoints_stay_removed(self) -> None:
        self.assertFalse((Path(__file__).resolve().parent / "_all_quick.py").exists())
        self.assertFalse((Path(__file__).resolve().parent / "run_match_prefix.py").exists())


if __name__ == "__main__":
    unittest.main()
