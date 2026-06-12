from __future__ import annotations

import base64
import csv
import hashlib
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path

from setup_and_pack import wheel_runtime_helper as _WHEEL_HELPER


class WheelRuntimeHelperTest(unittest.TestCase):
    def test_create_wheel_refreshes_record_after_binary_rewrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            wheel_path = root / "fluxon_pyo3-0.0.0-py3-none-any.whl"
            source_dir = root / "wheel_src"
            dist_info_dir = source_dir / "fluxon_pyo3-0.0.0.dist-info"
            pkg_dir = source_dir / "fluxon_pyo3"
            dist_info_dir.mkdir(parents=True)
            pkg_dir.mkdir(parents=True)
            payload_path = pkg_dir / "payload.bin"
            payload_path.write_bytes(b"old-payload")
            (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
            (dist_info_dir / "WHEEL").write_text("Wheel-Version: 1.0\n", encoding="utf-8")
            (dist_info_dir / "METADATA").write_text("Metadata-Version: 2.1\nName: fluxon-pyo3\nVersion: 0.0.0\n", encoding="utf-8")
            (dist_info_dir / "RECORD").write_text("stale,sha256=deadbeef,1\n", encoding="utf-8")

            payload_path.write_bytes(b"new-payload")
            _WHEEL_HELPER.create_wheel(str(wheel_path), str(source_dir))

            with zipfile.ZipFile(wheel_path, "r") as zip_ref:
                record_rows = list(csv.reader(zip_ref.read("fluxon_pyo3-0.0.0.dist-info/RECORD").decode("utf-8").splitlines()))
                record_map = {row[0]: row[1:] for row in record_rows}
                actual_payload = zip_ref.read("fluxon_pyo3/payload.bin")
                actual_hash = "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(actual_payload).digest()).decode("ascii").rstrip("=")
                self.assertEqual(record_map["fluxon_pyo3/payload.bin"], [actual_hash, str(len(actual_payload))])
                self.assertEqual(record_map["fluxon_pyo3-0.0.0.dist-info/RECORD"], ["", ""])

    def test_install_shared_libraries_overwrites_and_adds_members(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            wheel_path = root / "fluxon_pyo3-0.0.0-py3-none-any.whl"
            source_dir = root / "wheel_src"
            libs_dir = source_dir / "fluxon_pyo3.libs"
            libs_dir.mkdir(parents=True)
            (source_dir / "fluxon_pyo3").mkdir()
            (source_dir / "fluxon_pyo3" / "__init__.py").write_text("", encoding="utf-8")
            (libs_dir / "libfluxon_commu_core-abc.so").write_text("old-core", encoding="utf-8")
            _WHEEL_HELPER.create_wheel(str(wheel_path), str(source_dir))

            core_source = root / "libfluxon_commu_core.so"
            probe_source = root / "libfluxon_rdma_probe.so"
            core_source.write_text("new-core", encoding="utf-8")
            probe_source.write_text("new-probe", encoding="utf-8")

            _WHEEL_HELPER.install_shared_libraries(
                str(wheel_path),
                {
                    "libfluxon_commu_core-abc.so": str(core_source),
                    "libfluxon_rdma_probe.so": str(probe_source),
                },
            )

            with zipfile.ZipFile(wheel_path, "r") as zip_ref:
                self.assertEqual(
                    zip_ref.read("fluxon_pyo3.libs/libfluxon_commu_core-abc.so").decode("utf-8"),
                    "new-core",
                )
                self.assertEqual(
                    zip_ref.read("fluxon_pyo3.libs/libfluxon_rdma_probe.so").decode("utf-8"),
                    "new-probe",
                )

    def test_normalize_wheel_lib_rpaths_sets_extension_runpath_to_wheel_libs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            wheel_path = root / "fluxon_pyo3-0.0.0-py3-none-any.whl"
            source_dir = root / "wheel_src"
            pkg_dir = source_dir / "fluxon_pyo3"
            libs_dir = source_dir / "fluxon_pyo3.libs"
            pkg_dir.mkdir(parents=True)
            libs_dir.mkdir(parents=True)
            (pkg_dir / "__init__.py").write_text("", encoding="utf-8")

            ext_source = root / "libexample.so"
            subprocess.run(
                ["cc", "-shared", "-fPIC", "-Wl,-soname,libexample.so", "-o", str(ext_source), "-xc", "-"],
                input="int fluxon_dummy(void) { return 0; }\n",
                text=True,
                check=True,
            )
            (pkg_dir / "fluxon_pyo3.abi3.so").write_bytes(ext_source.read_bytes())
            (libs_dir / "libexample.so").write_bytes(ext_source.read_bytes())
            _WHEEL_HELPER.create_wheel(str(wheel_path), str(source_dir))

            _WHEEL_HELPER.normalize_wheel_lib_rpaths(str(wheel_path))

            extract_root = Path(_WHEEL_HELPER.extract_wheel(str(wheel_path)))
            try:
                ext_path = extract_root / "fluxon_pyo3" / "fluxon_pyo3.abi3.so"
                readelf_out = subprocess.run(
                    ["readelf", "-d", str(ext_path)],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout
                self.assertIn("$ORIGIN/../fluxon_pyo3.libs", readelf_out)
            finally:
                subprocess.run(["rm", "-rf", str(extract_root)], check=True)


if __name__ == "__main__":
    unittest.main()
