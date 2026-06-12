from __future__ import annotations

import os
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from setup_and_pack.scan_dir_size_progress import DirectorySizeScanner, _format_bytes


class FormatBytesTest(unittest.TestCase):
    def test_format_bytes(self) -> None:
        self.assertEqual(_format_bytes(0), "0 B")
        self.assertEqual(_format_bytes(1024), "1.00 KiB")


class DirectorySizeScannerTest(unittest.TestCase):
    def test_scan_apparent_size_counts_hardlink_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "a").mkdir()
            (root / "b").mkdir()
            payload = root / "a" / "data.bin"
            payload.write_bytes(b"x" * 17)
            os.link(payload, root / "b" / "data_link.bin")
            os.symlink(payload, root / "a" / "data.symlink")

            scanner = DirectorySizeScanner(
                root,
                threads=2,
                apparent_size=True,
                one_file_system=True,
                progress_interval_seconds=60.0,
                show_running_limit=4,
            )
            summary = scanner.scan()

            expected = 0
            seen_non_dir: set[tuple[int, int]] = set()
            for current_root, dir_names, file_names in os.walk(root, followlinks=False):
                current_path = Path(current_root)
                expected += current_path.lstat().st_size
                for file_name in file_names:
                    stat_result = (current_path / file_name).lstat()
                    key = (int(stat_result.st_dev), int(stat_result.st_ino))
                    if key in seen_non_dir:
                        continue
                    seen_non_dir.add(key)
                    expected += stat_result.st_size

            self.assertEqual(summary.total_bytes, expected)
            self.assertEqual(summary.files, 1)
            self.assertEqual(summary.symlinks, 1)
            self.assertEqual(summary.directories, 3)
            self.assertEqual(summary.errors, 0)


if __name__ == "__main__":
    unittest.main()
