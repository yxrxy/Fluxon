from __future__ import annotations

from pathlib import Path
import stat
import sys
import tempfile
import unittest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from setup_and_pack.rclone_sequential import RollingWindowLog, build_argument_parser, load_pair_file, run_sequence


class RollingWindowLogTest(unittest.TestCase):
    def test_keeps_only_recent_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "rolling.log"
            log = RollingWindowLog(log_path, max_lines=3)
            for index in range(5):
                log.write_line(f"line-{index}")
            self.assertEqual(
                log_path.read_text(encoding="utf-8").splitlines(),
                ["line-2", "line-3", "line-4"],
            )


class SequentialRunnerTest(unittest.TestCase):
    def test_load_pair_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pair_path = root / "pairs.txt"
            pair_path.write_text(
                "# comment\n"
                "src_a:/data/one\tdst_a:/data/one\n"
                "src_b:/data/two dst_b:/data/two\n",
                encoding="utf-8",
            )
            tasks = load_pair_file(pair_path)
            self.assertEqual(len(tasks), 2)
            self.assertEqual(tasks[0].src, "src_a:/data/one")
            self.assertEqual(tasks[0].dst, "dst_a:/data/one")
            self.assertEqual(tasks[1].src, "src_b:/data/two")
            self.assertEqual(tasks[1].dst, "dst_b:/data/two")

    def test_load_pair_file_accepts_space_in_src_when_dst_is_single_remote_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pair_path = root / "pairs.txt"
            src = (
                "/nvfile-heatstorage/nvfile-coldstorage/basemodel_data2/"
                "07月02日_下午_室内_模拟农家乐环境1_单人_独立运动类/"
                "道具交互类/ more words"
            )
            dst = (
                "lingang8:/data/transfer_data/aigc/basemodel_data2/"
                "07月02日_下午_室内_模拟农家乐环境1_单人_独立运动类/"
                "道具交互类/"
            )
            pair_path.write_text(f"{src} {dst}\n", encoding="utf-8")

            tasks = load_pair_file(pair_path)

            self.assertEqual(len(tasks), 1)
            self.assertEqual(tasks[0].src, src)
            self.assertEqual(tasks[0].dst, dst)

    def test_load_pair_file_accepts_single_explicit_multi_space_separator(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pair_path = root / "pairs.txt"
            src = "/src path/with spaces/file one"
            dst = "lingang8:/dst path/with spaces/file one"
            pair_path.write_text(f"{src}   {dst}\n", encoding="utf-8")

            tasks = load_pair_file(pair_path)

            self.assertEqual(len(tasks), 1)
            self.assertEqual(tasks[0].src, src)
            self.assertEqual(tasks[0].dst, dst)

    def test_load_pair_file_rejects_ambiguous_whitespace_only_line(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pair_path = root / "pairs.txt"
            pair_path.write_text("src with spaces dst with spaces\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "prefer TAB delimiter"):
                load_pair_file(pair_path)

    def test_load_pair_file_rejects_multiple_explicit_multi_space_separators(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pair_path = root / "pairs.txt"
            pair_path.write_text("src  with spaces   lingang8:/dst path\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "2\\+ space separator"):
                load_pair_file(pair_path)

    def test_runs_pair_file_in_order_and_limits_log_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pair_path = root / "pairs.txt"
            pair_path.write_text(
                "src_remote:/data/alpha/one\tdst_remote:/data/alpha/one\n"
                "src_remote:/data/beta/two\tdst_remote:/data/beta/two\n",
                encoding="utf-8",
            )

            fake_rclone = root / "fake_rclone.sh"
            fake_rclone.write_text(
                "#!/usr/bin/env bash\n"
                "echo start:$1:$2:$3\n"
                "echo done:$1:$2:$3\n",
                encoding="utf-8",
            )
            fake_rclone.chmod(fake_rclone.stat().st_mode | stat.S_IXUSR)

            log_dir = root / "logs"
            parser = build_argument_parser()
            args = parser.parse_args(
                [
                    "--pair-file",
                    str(pair_path),
                    "--rclone-bin",
                    str(fake_rclone),
                    "--log-dir",
                    str(log_dir),
                    "--log-window-lines",
                    "5",
                ]
            )

            rc = run_sequence(args)

            self.assertEqual(rc, 0)
            runner_lines = (log_dir / "runner.log").read_text(encoding="utf-8").splitlines()
            task_logs = sorted(log_dir.glob("task_*.log"))
            self.assertEqual(len(task_logs), 2)
            self.assertLessEqual(len(runner_lines), 5)
            self.assertTrue(any("src_remote:/data/beta/two -> dst_remote:/data/beta/two" in line for line in runner_lines))
            self.assertTrue(runner_lines[-1].endswith("[done] total=2 succeeded=2 failed=0"))
            for task_log in task_logs:
                task_lines = task_log.read_text(encoding="utf-8").splitlines()
                self.assertGreaterEqual(len(task_lines), 3)
                self.assertLessEqual(len(task_lines), 5)

    def test_max_inflight_allows_parallel_rclone_processes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pair_path = root / "pairs.txt"
            pair_path.write_text(
                "src_remote:/data/alpha/one\tdst_remote:/data/alpha/one\n"
                "src_remote:/data/beta/two\tdst_remote:/data/beta/two\n",
                encoding="utf-8",
            )

            fake_rclone = root / "fake_rclone.sh"
            fake_rclone.write_text(
                "#!/usr/bin/env bash\n"
                "sleep 0.5\n"
                "echo done:$2\n",
                encoding="utf-8",
            )
            fake_rclone.chmod(fake_rclone.stat().st_mode | stat.S_IXUSR)

            log_dir = root / "logs"
            parser = build_argument_parser()
            args = parser.parse_args(
                [
                    "--pair-file",
                    str(pair_path),
                    "--rclone-bin",
                    str(fake_rclone),
                    "--log-dir",
                    str(log_dir),
                    "--max-inflight",
                    "2",
                ]
            )

            rc = run_sequence(args)

            self.assertEqual(rc, 0)
            runner_lines = (log_dir / "runner.log").read_text(encoding="utf-8").splitlines()
            self.assertTrue(any("max_inflight=2" in line for line in runner_lines))
            start_positions = [index for index, line in enumerate(runner_lines) if "[task-start]" in line]
            done_positions = [index for index, line in enumerate(runner_lines) if "[task-done]" in line]
            self.assertEqual(len(start_positions), 2)
            self.assertEqual(len(done_positions), 2)
            self.assertLess(start_positions[1], done_positions[0])


if __name__ == "__main__":
    unittest.main()
