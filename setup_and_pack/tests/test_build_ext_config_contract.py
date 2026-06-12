import json
import subprocess
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[2]
TEST_CRATE_DIR = Path(__file__).resolve().parent / "build_ext_config_contract_rust"


def write_build_config(etcd_value: str, prom_base_url: str, prom_remote_write_url: str) -> Path:
    cfg_path = ROOT / "build_config_ext.yml"
    cfg_path.write_text(
        f"""
etcd: {etcd_value}
prom: {prom_base_url}
prom_remote_write_url: {prom_remote_write_url}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return cfg_path


def read_python_config():
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from setup_and_pack.utils.repo_config_utils import (
        load_etcd_config,
        load_tsdb_base_url,
        load_tsdb_remote_write_url,
    )

    etcd = load_etcd_config()
    prom_base = load_tsdb_base_url()
    prom_remote_write = load_tsdb_remote_write_url()
    return etcd, prom_base, prom_remote_write


def read_rust_config() -> dict:
    # Invoke the small Rust helper and parse its JSON output
    exe = [
        "cargo",
        "run",
        "--quiet",
        "--manifest-path",
        str(TEST_CRATE_DIR / "Cargo.toml"),
    ]
    out = subprocess.check_output(exe, cwd=str(ROOT))
    return json.loads(out.decode().strip())


class BuildExtConfigContractTest(unittest.TestCase):
    def _assert_contract(self, etcd_value: str) -> None:
        cfg_path = ROOT / "build_config_ext.yml"
        backup_path = None
        if cfg_path.exists():
            backup_path = ROOT / "build_config_ext.yml.bak"
            cfg_path.replace(backup_path)

        try:
            prom_base_url = "http://127.0.0.1:19090/api/v1"
            prom_remote_write_url = "http://127.0.0.1:19090/api/v1/write"
            write_build_config(etcd_value, prom_base_url, prom_remote_write_url)

            py_etcd, py_prom_base, py_prom_remote_write = read_python_config()
            rust_json = read_rust_config()
            rs_etcd = rust_json["etcd"]
            rs_prom_base = rust_json["prom"]
            rs_prom_remote_write = rust_json["prom_remote_write_url"]

            self.assertEqual(py_prom_base, prom_base_url)
            self.assertEqual(rs_prom_base, prom_base_url)
            self.assertEqual(py_prom_remote_write, prom_remote_write_url)
            self.assertEqual(rs_prom_remote_write, prom_remote_write_url)

            self.assertEqual(py_etcd, etcd_value)
            if etcd_value.startswith(("http://", "https://")):
                self.assertEqual(rs_etcd, etcd_value)
            else:
                self.assertEqual(rs_etcd, f"http://{py_etcd}")
        finally:
            if cfg_path.exists():
                cfg_path.unlink()
            if backup_path and backup_path.exists():
                backup_path.replace(cfg_path)

    def test_build_ext_config_contract(self) -> None:
        for etcd_value in (
            "127.0.0.1:2379",
        ):
            with self.subTest(etcd_value=etcd_value):
                self._assert_contract(etcd_value)


if __name__ == "__main__":
    unittest.main()
