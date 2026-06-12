import hashlib
import shutil
import sys
import time
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY = 10
LARGE_FLAT_TRANSFER_FILE_COUNT = 4200
LARGE_FLAT_TRANSFER_FILE_BYTES = 1024
LARGE_FLAT_TRANSFER_BATCH_READY_BYTES = 64 * 1024
LARGE_FANOUT_TRANSFER_CHILD_DIR_COUNT = 4200
LARGE_FANOUT_TRANSFER_FILE_BYTES = 16


def main() -> None:
    unittest.main()


from fluxon_py.fluxon_fs import (  # noqa: E402
    transfer_inspect_local_job_blocking,
)
from fluxon_fs_transfer_tikv_support import (  # noqa: E402
    FluxonFsRemoteWholeHarness,
    HEARTBEAT_RETRY_PAUSE_SECS,
    PreparedTransferFixture,
    READ_RPC_RETRY_OUTAGE_SECS,
    SCAN_BATCH_READY_BYTES,
    TRANSFER_CHUNK_BYTES,
    collect_collect_info_output_paths,
    collect_directory_relpaths,
    collect_regular_file_signatures,
    has_fluxon_pyo3,
    new_test_dir,
    new_shm_test_dir,
    prepare_transfer_fixture_once,
    validate_transfer_job_batches_against_source,
    wait_for_staging_file_at_least_bytes,
)


def _expected_directory_relpaths_for_fixture(fixture: PreparedTransferFixture) -> list[str]:
    out = set(fixture.expected_empty_dirs)
    for relpath in fixture.expected_entries:
        parent = Path(relpath).parent.as_posix()
        while parent != ".":
            out.add(parent)
            parent = Path(parent).parent.as_posix()
    return sorted(out)


def _assert_completed_transfer_result_for_fixture(
    *,
    fixture: PreparedTransferFixture,
    dst_root: Path,
    remote: FluxonFsRemoteWholeHarness,
    result: dict[str, object],
) -> None:
    job = result["job"]
    created_job = result["created_job"]
    assert created_job["desired_worker_count"] == 10
    assert job["job_state"] == "completed"
    assert job["open_batches"] == 0
    assert all(batch["state"] == "finished" for batch in job["batches"])
    assert all(row["materialized"] for row in job["collect_infos"])

    inspected_job = transfer_inspect_local_job_blocking(
        transfer_state_store=remote.store_config,
        job_id=str(created_job["job_id"]),
    )
    assert inspected_job == job

    validation = validate_transfer_job_batches_against_source(
        job=job,
        src_root=fixture.src_root,
        skip_entries=fixture.skip_entries,
    )
    assert validation["expected_entries"] == fixture.expected_entries
    assert validation["expected_empty_dirs"] == fixture.expected_empty_dirs
    expected_signatures = {
        relpath: (
            fixture.expected_entries[relpath],
            fixture.expected_sha256[relpath],
        )
        for relpath in sorted(fixture.expected_entries)
    }
    actual_signatures = collect_regular_file_signatures(
        dst_root,
        ignored_top_level_dirs=("fluxon_collect_info", ".fluxon.stage"),
    )
    assert actual_signatures == expected_signatures
    assert collect_directory_relpaths(
        dst_root,
        ignored_top_level_dirs=("fluxon_collect_info", ".fluxon.stage"),
    ) == _expected_directory_relpaths_for_fixture(fixture)

    assert collect_collect_info_output_paths(dst_root) == [
        f"fluxon_collect_info/batches/{row['batch_id']}/symlinks.jsonl"
        for row in sorted(job["collect_infos"], key=lambda row: row["batch_id"])
    ]
    for row in job["collect_infos"]:
        path = (
            dst_root
            / "fluxon_collect_info"
            / "batches"
            / row["batch_id"]
            / "symlinks.jsonl"
        )
        assert path.is_file()
        assert path.stat().st_size > 0


def _build_large_flat_fixture(root: Path) -> PreparedTransferFixture:
    src_root = root / "src"
    src_root.mkdir(parents=True, exist_ok=False)
    expected_entries: dict[str, int] = {}
    expected_sha256: dict[str, str] = {}
    for index in range(LARGE_FLAT_TRANSFER_FILE_COUNT):
        relpath = f"root/file_{index:05d}.bin"
        content = bytes([(index % 251) + 1]) * LARGE_FLAT_TRANSFER_FILE_BYTES
        path = src_root / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        expected_entries[relpath] = len(content)
        expected_sha256[relpath] = hashlib.sha256(content).hexdigest()
    return PreparedTransferFixture(
        src_root=src_root,
        skip_entries=[],
        expected_entries=expected_entries,
        expected_sha256=expected_sha256,
        expected_empty_dirs=[],
    )


def _build_large_fanout_fixture(root: Path) -> PreparedTransferFixture:
    src_root = root / "src"
    src_root.mkdir(parents=True, exist_ok=False)
    expected_entries: dict[str, int] = {}
    expected_sha256: dict[str, str] = {}
    for index in range(LARGE_FANOUT_TRANSFER_CHILD_DIR_COUNT):
        relpath = f"root/child_{index:05d}/payload.bin"
        content = bytes([(index % 251) + 1]) * LARGE_FANOUT_TRANSFER_FILE_BYTES
        path = src_root / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        expected_entries[relpath] = len(content)
        expected_sha256[relpath] = hashlib.sha256(content).hexdigest()
    return PreparedTransferFixture(
        src_root=src_root,
        skip_entries=[],
        expected_entries=expected_entries,
        expected_sha256=expected_sha256,
        expected_empty_dirs=[],
    )


class TestFluxonFsTransferWholeTiKv(unittest.TestCase):
    def _expected_directory_relpaths(self) -> list[str]:
        return _expected_directory_relpaths_for_fixture(self._fixture)

    def setUp(self) -> None:
        self._tmp = new_test_dir("transfer_whole_tikv")
        self.addCleanup(lambda: shutil.rmtree(self._tmp, ignore_errors=False))
        self._dst_tmp = new_shm_test_dir("transfer_whole_tikv_dst")
        self.addCleanup(lambda: shutil.rmtree(self._dst_tmp, ignore_errors=False))
        if not has_fluxon_pyo3():
            self.skipTest("missing runtime dependency: fluxon_pyo3")
        self._fixture = prepare_transfer_fixture_once()
        self._dst_root = self._dst_tmp / "dst"
        self._dst_root.mkdir(parents=True, exist_ok=False)
        try:
            self._remote = FluxonFsRemoteWholeHarness(
                tag="fluxon_fs_transfer_whole_tikv",
                work_root=self._tmp / "remote_stack",
                fixture=self._fixture,
                dst_root=self._dst_root,
            )
        except FileNotFoundError as err:
            self.skipTest(str(err))
        self.addCleanup(self._close_remote)

    def _close_remote(self) -> None:
        if hasattr(self, "_remote"):
            self._remote.close()

    def _assert_completed_transfer_result(self, result: dict[str, object]) -> None:
        _assert_completed_transfer_result_for_fixture(
            fixture=self._fixture,
            dst_root=self._dst_root,
            remote=self._remote,
            result=result,
        )

    def test_runs_to_completion(self) -> None:
        self.assertEqual(list(self._dst_root.iterdir()), [])
        result = self._remote.run_transfer_job(
            desired_scan_concurrency=TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
            desired_worker_count=10,
            batch_ready_bytes=SCAN_BATCH_READY_BYTES,
            skip_entries=self._fixture.skip_entries,
        )
        self._assert_completed_transfer_result(result)

    def test_runs_to_completion_after_src_agent_restart_during_read_rpc(self) -> None:
        self.assertEqual(list(self._dst_root.iterdir()), [])
        created_job = self._remote.create_transfer_job(
            desired_scan_concurrency=TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
            desired_worker_count=10,
            batch_ready_bytes=SCAN_BATCH_READY_BYTES,
            skip_entries=self._fixture.skip_entries,
        )
        job_id = str(created_job["job_id"])
        self._remote.wait_for_transfer_running(job_id=job_id)
        wait_for_staging_file_at_least_bytes(
            self._dst_root,
            min_size_bytes=TRANSFER_CHUNK_BYTES,
            timeout_secs=60.0,
        )
        self._remote.stop_src_agent()
        time.sleep(READ_RPC_RETRY_OUTAGE_SECS)
        self._remote.start_src_agent()
        job = self._remote.wait_for_transfer_completion(job_id=job_id)
        self._assert_completed_transfer_result(
            {
                "created_job": created_job,
                "job": job,
            }
        )

    def test_runs_to_completion_after_fs_master_pause_during_heartbeat_rpc(self) -> None:
        self.assertEqual(list(self._dst_root.iterdir()), [])
        created_job = self._remote.create_transfer_job(
            desired_scan_concurrency=TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
            desired_worker_count=10,
            batch_ready_bytes=SCAN_BATCH_READY_BYTES,
            skip_entries=self._fixture.skip_entries,
        )
        job_id = str(created_job["job_id"])
        self._remote.wait_for_transfer_running(job_id=job_id)
        self._remote.pause_fs_master()
        try:
            time.sleep(HEARTBEAT_RETRY_PAUSE_SECS)
        finally:
            self._remote.resume_fs_master()
        job = self._remote.wait_for_transfer_completion(job_id=job_id)
        self._assert_completed_transfer_result(
            {
                "created_job": created_job,
                "job": job,
            }
        )


class TestFluxonFsTransferWholeLargeFlatDirTiKv(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = new_test_dir("transfer_whole_large_flat_tikv")
        self.addCleanup(lambda: shutil.rmtree(self._tmp, ignore_errors=False))
        self._dst_tmp = new_shm_test_dir("transfer_whole_large_flat_tikv_dst")
        self.addCleanup(lambda: shutil.rmtree(self._dst_tmp, ignore_errors=False))
        if not has_fluxon_pyo3():
            self.skipTest("missing runtime dependency: fluxon_pyo3")
        self._fixture = _build_large_flat_fixture(self._tmp / "fixture")
        self._dst_root = self._dst_tmp / "dst"
        self._dst_root.mkdir(parents=True, exist_ok=False)
        try:
            self._remote = FluxonFsRemoteWholeHarness(
                tag="fluxon_fs_transfer_whole_large_flat_tikv",
                work_root=self._tmp / "remote_stack",
                fixture=self._fixture,
                dst_root=self._dst_root,
            )
        except FileNotFoundError as err:
            self.skipTest(str(err))
        self.addCleanup(self._close_remote)

    def _close_remote(self) -> None:
        if hasattr(self, "_remote"):
            self._remote.close()

    def test_runs_to_completion_for_large_flat_directory(self) -> None:
        self.assertEqual(list(self._dst_root.iterdir()), [])
        result = self._remote.run_transfer_job(
            desired_scan_concurrency=TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
            desired_worker_count=10,
            batch_ready_bytes=LARGE_FLAT_TRANSFER_BATCH_READY_BYTES,
            skip_entries=self._fixture.skip_entries,
        )
        _assert_completed_transfer_result_for_fixture(
            fixture=self._fixture,
            dst_root=self._dst_root,
            remote=self._remote,
            result=result,
        )


class TestFluxonFsTransferWholeLargeFanoutDirTiKv(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = new_test_dir("transfer_whole_large_fanout_tikv")
        self.addCleanup(lambda: shutil.rmtree(self._tmp, ignore_errors=False))
        self._dst_tmp = new_shm_test_dir("transfer_whole_large_fanout_tikv_dst")
        self.addCleanup(lambda: shutil.rmtree(self._dst_tmp, ignore_errors=False))
        if not has_fluxon_pyo3():
            self.skipTest("missing runtime dependency: fluxon_pyo3")
        self._fixture = _build_large_fanout_fixture(self._tmp / "fixture")
        self._dst_root = self._dst_tmp / "dst"
        self._dst_root.mkdir(parents=True, exist_ok=False)
        try:
            self._remote = FluxonFsRemoteWholeHarness(
                tag="fluxon_fs_transfer_whole_large_fanout_tikv",
                work_root=self._tmp / "remote_stack",
                fixture=self._fixture,
                dst_root=self._dst_root,
            )
        except FileNotFoundError as err:
            self.skipTest(str(err))
        self.addCleanup(self._close_remote)

    def _close_remote(self) -> None:
        if hasattr(self, "_remote"):
            self._remote.close()

    def test_runs_to_completion_for_large_root_child_fanout(self) -> None:
        self.assertEqual(list(self._dst_root.iterdir()), [])
        result = self._remote.run_transfer_job(
            desired_scan_concurrency=TRANSFER_JOB_DESIRED_SCAN_CONCURRENCY,
            desired_worker_count=10,
            batch_ready_bytes=SCAN_BATCH_READY_BYTES,
            skip_entries=self._fixture.skip_entries,
        )
        _assert_completed_transfer_result_for_fixture(
            fixture=self._fixture,
            dst_root=self._dst_root,
            remote=self._remote,
            result=result,
        )


if __name__ == "__main__":
    main()
