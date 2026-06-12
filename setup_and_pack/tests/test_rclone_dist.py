from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from setup_and_pack.rclone_dist.common import (
    TaskSpec,
    TaskStore,
    build_rclone_dir_command,
    join_rclone_root,
    load_task_manifest,
)


class ManifestTest(unittest.TestCase):
    def test_load_task_manifest_ignores_blank_comment_and_duplicate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "manifest.txt"
            manifest_path.write_text(
                "\n".join(
                    [
                        "",
                        "# comment",
                        "alpha/one",
                        "alpha/one",
                        "/beta/two/",
                        "gamma\\three",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            tasks = load_task_manifest(manifest_path)

        self.assertEqual(
            [task.relative_path for task in tasks],
            ["alpha/one", "beta/two", "gamma/three"],
        )


class JoinAndCommandTest(unittest.TestCase):
    def test_join_local_root(self) -> None:
        self.assertEqual(join_rclone_root("/tmp/root", "a/b"), "/tmp/root/a/b")

    def test_join_remote_root(self) -> None:
        self.assertEqual(join_rclone_root("src_remote:prefix", "a/b"), "src_remote:prefix/a/b")

    def test_build_rclone_dir_command(self) -> None:
        self.assertEqual(
            build_rclone_dir_command(
                rclone_bin="rclone",
                src_root="src_remote:root",
                dst_root="dst_remote:root",
                relative_path="alpha/one",
                rclone_args=["--transfers=1"],
            ),
            [
                "rclone",
                "copy",
                "src_remote:root/alpha/one",
                "dst_remote:root/alpha/one",
                "--transfers=1",
            ],
        )


class TaskStoreTest(unittest.TestCase):
    def _build_store(self) -> TaskStore:
        return TaskStore(
            [TaskSpec(task_id=1, relative_path="alpha/one")],
            max_attempts=2,
            low_bandwidth_threshold_bps=100.0,
            bandwidth_sustain_seconds=10.0,
            min_bandwidth_samples=2,
        )

    def _build_retry_store(self) -> TaskStore:
        return TaskStore(
            [TaskSpec(task_id=1, relative_path="alpha/one")],
            max_attempts=2,
            low_bandwidth_threshold_bps=100.0,
            bandwidth_sustain_seconds=0.0,
            min_bandwidth_samples=1,
        )

    def test_bandwidth_gate_requires_sustained_low_window(self) -> None:
        store = self._build_store()

        with mock.patch("setup_and_pack.rclone_dist.common.time.time", return_value=100.0):
            task, lease, all_done, reason, gate = store.acquire(
                worker_id="worker-a",
                peer_id="peer-a",
                lease_seconds=30,
                downlink_bps=90.0,
            )
        self.assertIsNone(task)
        self.assertIsNone(lease)
        self.assertFalse(all_done)
        self.assertEqual(reason, "insufficient_bandwidth_samples")
        self.assertFalse(gate["allowed"])

        with mock.patch("setup_and_pack.rclone_dist.common.time.time", return_value=110.0):
            task, lease, all_done, reason, gate = store.acquire(
                worker_id="worker-a",
                peer_id="peer-a",
                lease_seconds=30,
                downlink_bps=80.0,
            )
        self.assertFalse(all_done)
        self.assertEqual(reason, "granted")
        self.assertTrue(gate["allowed"])
        self.assertIsNotNone(task)
        self.assertIsNotNone(lease)
        self.assertEqual(task.relative_path, "alpha/one")
        self.assertEqual(lease.attempt, 1)

    def test_high_bandwidth_keeps_gate_closed(self) -> None:
        store = self._build_store()

        with mock.patch("setup_and_pack.rclone_dist.common.time.time", return_value=100.0):
            store.acquire(worker_id="worker-a", peer_id="peer-a", lease_seconds=30, downlink_bps=50.0)
        with mock.patch("setup_and_pack.rclone_dist.common.time.time", return_value=105.0):
            store.acquire(worker_id="worker-a", peer_id="peer-a", lease_seconds=30, downlink_bps=150.0)
        with mock.patch("setup_and_pack.rclone_dist.common.time.time", return_value=110.0):
            task, lease, all_done, reason, gate = store.acquire(
                worker_id="worker-a",
                peer_id="peer-a",
                lease_seconds=30,
                downlink_bps=50.0,
            )
        self.assertIsNone(task)
        self.assertIsNone(lease)
        self.assertFalse(all_done)
        self.assertEqual(reason, "bandwidth_too_high")
        self.assertFalse(gate["allowed"])

    def test_failure_requeues_then_success_completes(self) -> None:
        store = self._build_retry_store()

        with mock.patch("setup_and_pack.rclone_dist.common.time.time", return_value=100.0):
            task, lease, _, _, _ = store.acquire(
                worker_id="worker-a",
                peer_id="peer-a",
                lease_seconds=30,
                downlink_bps=90.0,
            )
        self.assertIsNotNone(task)
        self.assertIsNotNone(lease)

        ok, note = store.ack_launch(
            task_id=1,
            worker_id="worker-a",
            session_name="tmux-1",
            command=["rclone", "copy"],
            note="launched",
        )
        self.assertTrue(ok)
        self.assertEqual(note, "launch recorded")

        ok, note = store.report_finish(
            task_id=1,
            worker_id="worker-a",
            peer_id="peer-a",
            return_code=9,
            finished_at_unix=120.0,
            elapsed_seconds=10.0,
            session_name="tmux-1",
            command=["rclone", "copy"],
            note="first failure",
        )
        self.assertTrue(ok)
        self.assertEqual(note, "requeued after failure")

        with mock.patch("setup_and_pack.rclone_dist.common.time.time", return_value=130.0):
            task, lease, _, _, _ = store.acquire(
                worker_id="worker-b",
                peer_id="peer-a",
                lease_seconds=30,
                downlink_bps=70.0,
            )
        self.assertIsNotNone(task)
        self.assertIsNotNone(lease)
        self.assertEqual(lease.attempt, 2)

        ok, note = store.report_finish(
            task_id=1,
            worker_id="worker-b",
            peer_id="peer-a",
            return_code=0,
            finished_at_unix=140.0,
            elapsed_seconds=10.0,
            session_name="tmux-2",
            command=["rclone", "copy"],
            note="success",
        )
        self.assertTrue(ok)
        self.assertEqual(note, "recorded success")
        snapshot = store.status_snapshot()
        self.assertEqual(snapshot["succeeded_tasks"], 1)
        self.assertTrue(snapshot["all_done"])

    def test_expired_lease_is_requeued(self) -> None:
        store = self._build_retry_store()

        with mock.patch("setup_and_pack.rclone_dist.common.time.time", return_value=100.0):
            task, lease, _, _, _ = store.acquire(
                worker_id="worker-a",
                peer_id="peer-a",
                lease_seconds=5,
                downlink_bps=90.0,
            )
        self.assertIsNotNone(task)
        self.assertIsNotNone(lease)
        self.assertEqual(lease.attempt, 1)

        with mock.patch("setup_and_pack.rclone_dist.common.time.time", return_value=117.0):
            task, lease, _, _, _ = store.acquire(
                worker_id="worker-b",
                peer_id="peer-a",
                lease_seconds=5,
                downlink_bps=70.0,
            )
        self.assertIsNotNone(task)
        self.assertIsNotNone(lease)
        self.assertEqual(lease.attempt, 2)


if __name__ == "__main__":
    unittest.main()
