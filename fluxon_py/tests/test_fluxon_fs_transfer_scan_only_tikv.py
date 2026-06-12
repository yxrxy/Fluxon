import shutil
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))


def main() -> None:
    unittest.main()


from fluxon_py.fluxon_fs import (  # noqa: E402
    transfer_check_local_blocking,
    transfer_inspect_local_job_blocking,
)
from fluxon_fs_transfer_tikv_support import (  # noqa: E402
    SCAN_BATCH_READY_BYTES,
    TiKvHarness,
    has_fluxon_pyo3,
    new_test_dir,
    prepare_transfer_fixture_once,
    validate_transfer_job_batches_against_source,
)


class TestFluxonFsTransferScanOnlyTiKv(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = new_test_dir("transfer_scan_only_tikv")
        self.addCleanup(lambda: shutil.rmtree(self._tmp, ignore_errors=False))
        if not has_fluxon_pyo3():
            self.skipTest("missing runtime dependency: fluxon_pyo3")
        try:
            self._tikv = TiKvHarness(tag="fluxon_fs_transfer_scan_only_tikv")
        except FileNotFoundError as err:
            self.skipTest(str(err))
        self.addCleanup(self._close_tikv)

    def _close_tikv(self) -> None:
        if hasattr(self, "_tikv"):
            self._tikv.close()

    def test_collects_batches_covering_expected_source_set(self) -> None:
        fixture = prepare_transfer_fixture_once()
        store_config = self._tikv.build_store_config(key_suffix="scan_only")
        self._tikv.wait_until_store_ready(store_config=store_config)

        summary = transfer_check_local_blocking(
            src_root_dir=str(fixture.src_root),
            transfer_state_store=store_config,
            batch_ready_bytes=SCAN_BATCH_READY_BYTES,
            skip_entries=fixture.skip_entries,
            checker_concurrency_limit=4,
            enable_cli_progress=False,
        )

        job = transfer_inspect_local_job_blocking(
            transfer_state_store=store_config,
            job_id=str(summary["job_id"]),
        )
        self.assertTrue(job["scan_finished"])
        self.assertEqual(job["job_state"], "running")
        self.assertEqual(job["open_batches"], len(job["batches"]))
        self.assertEqual(summary["batch_count"], len(job["batches"]))

        validation = validate_transfer_job_batches_against_source(
            job=job,
            src_root=fixture.src_root,
            skip_entries=fixture.skip_entries,
        )
        self.assertEqual(validation["expected_entries"], fixture.expected_entries)
        self.assertEqual(validation["expected_empty_dirs"], fixture.expected_empty_dirs)
        self.assertEqual(
            summary["full_dir_batch_count"],
            validation["batch_kind_counts"]["full_dir"],
        )
        self.assertEqual(
            summary["direct_files_only_batch_count"],
            validation["batch_kind_counts"]["direct_files_only"],
        )
        self.assertTrue(
            any(
                total_bytes >= SCAN_BATCH_READY_BYTES
                for total_bytes in validation["batch_total_sizes"].values()
            )
        )

        collect_infos = sorted(
            job["collect_infos"],
            key=lambda row: (row["batch_id"], row["collect_kind"]),
        )
        self.assertGreaterEqual(len(collect_infos), 1)
        self.assertTrue(all(row["collect_kind"] == "symlink_notice" for row in collect_infos))
        self.assertTrue(all(int(row["collect_blob_bytes"]) > 0 for row in collect_infos))
        self.assertTrue(all(not row["materialized"] for row in collect_infos))


if __name__ == "__main__":
    main()
