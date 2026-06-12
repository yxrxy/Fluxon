import os
import sys
import time
import shutil
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))


def main() -> None:
    unittest.main()


from fluxon_py.fluxon_fs.config_types import (  # noqa: E402
    CacheMode,
    FS_EXPORT_CACHE_BYTES_FIELD_KEY,
    FluxonFsAccessModel,
    FluxonFsAccessUser,
    FluxonFsMasterPanelConfig,
    FluxonFsScopeAccess,
    FluxonFsScopeAccessMode,
    OnRefreshError,
    WriteMode,
    FluxonFsTransferSkipEntry,
    FluxonFsTransferSkipEntryKind,
    FluxonFsTransferStateStoreConfig,
    FluxonFsTransferStateStoreKind,
    FluxonFsTransferStateStoreTiKvConfig,
    export_cache_kv_key_prefix_for_export_name_v1,
    export_rpc_paths_for_export_name_v1,
    FluxonFsExportRoutingMode,
    extract_global_config_yaml_from_file,
    parse_global_config_from_yaml_text,
    parse_master_config_from_file,
    parse_master_panel_config_from_file,
    transfer_skip_entry_to_json_obj,
    transfer_state_store_to_json_text,
)


def _new_test_dir(tag: str) -> Path:
    base = REPO_ROOT / "fluxon_py" / "tests" / ".tmp_fluxon_fs"
    base.mkdir(parents=True, exist_ok=True)
    p = base / f"{tag}_{int(time.time() * 1000)}_{os.getpid()}"
    p.mkdir(parents=True, exist_ok=False)
    return p


class TestFluxonFsConfigTypes(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = _new_test_dir("fs_config_types")
        self.addCleanup(lambda: shutil.rmtree(self._tmp, ignore_errors=False))

    def test_parse_master_config_rejects_removed_rpc_timeout_ms(self) -> None:
        cfg_path = self._tmp / "cfg.yml"
        cfg_path.write_text(
            "\n".join(
                [
                    "fluxon_fs:",
                    "  master:",
                    "    instance_key: master",
                    "    pull_interval_ms: 1000",
                    "    rpc_timeout_ms: 10",
                ]
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, r"rpc_timeout_ms is removed"):
            _ = parse_master_config_from_file(cfg_path)

    def test_parse_master_config_accepts_instance_key_only(self) -> None:
        cfg_path = self._tmp / "cfg_master.yml"
        cfg_path.write_text(
            "\n".join(
                [
                    "fluxon_fs:",
                    "  master:",
                    "    instance_key: master",
                ]
            ),
            encoding="utf-8",
        )
        cfg = parse_master_config_from_file(cfg_path)
        self.assertEqual(cfg.instance_key, "master")
        self.assertIsNone(cfg.pull_interval_ms)

    def test_extract_global_config_yaml_from_file_returns_cache_mapping(self) -> None:
        cfg_path = self._tmp / "cfg.yml"
        cfg_path.write_text(
            "\n".join(
                [
                    "kvclient:",
                    "  instance_key: ignored",
                    "fluxon_fs:",
                    "  master:",
                    "    instance_key: master",
                    "  cache:",
                    "    stale_window_ms: 1000",
                    "    rules: []",
                    "    exports: {}",
                ]
            ),
            encoding="utf-8",
        )
        cache_yaml = extract_global_config_yaml_from_file(cfg_path)

        # The extracted YAML is the `fluxon_fs.cache` mapping, not a wrapper config.
        parsed = parse_global_config_from_yaml_text(cache_yaml)
        self.assertEqual(parsed.stale_window_ms, 1000)
        self.assertEqual(parsed.rules, [])
        self.assertEqual(parsed.exports, {})

    def test_parse_global_config_from_yaml_text_parses_rules_and_exports(self) -> None:
        cache_yaml = "\n".join(
            [
                "stale_window_ms: 1000",
                "rules:",
                "  - dir_abs: /abs/dir",
                "    cache_mode: read_through",
                "    write_mode: write_through",
                "    kv_key_prefix: /cache/",
                "    bytes_field_key: payload",
                "    max_cache_bytes: 16",
                "    on_refresh_error: apply_stale_window",
                "exports:",
                "  exp1:",
                "    remote_root_dir_abs: /abs/remote",
                "    nodes: [node1]",
                "    cache_max_bytes: 32",
            ]
        )
        cfg = parse_global_config_from_yaml_text(cache_yaml)
        self.assertEqual(cfg.stale_window_ms, 1000)
        self.assertEqual(len(cfg.rules), 1)
        self.assertEqual(cfg.rules[0].cache_mode, CacheMode.READ_THROUGH)
        self.assertEqual(cfg.rules[0].write_mode, WriteMode.WRITE_THROUGH)
        self.assertEqual(cfg.rules[0].on_refresh_error, OnRefreshError.APPLY_STALE_WINDOW)
        self.assertIn("exp1", cfg.exports)
        self.assertEqual(cfg.exports["exp1"].routing_mode, FluxonFsExportRoutingMode.STATIC_NODES)
        self.assertEqual(
            cfg.exports["exp1"].cache_kv_key_prefix,
            export_cache_kv_key_prefix_for_export_name_v1("exp1"),
        )
        self.assertEqual(cfg.exports["exp1"].cache_bytes_field_key, FS_EXPORT_CACHE_BYTES_FIELD_KEY)
        self.assertEqual(
            cfg.exports["exp1"].rpc_paths.stat,
            export_rpc_paths_for_export_name_v1("exp1").stat,
        )

    def test_parse_global_config_accepts_omitted_rules(self) -> None:
        cache_yaml = "\n".join(
            [
                "stale_window_ms: 1000",
                "exports:",
                "  exp1:",
                "    remote_root_dir_abs: /abs/remote",
                "    nodes: [node1]",
                "    cache_max_bytes: 32",
            ]
        )
        cfg = parse_global_config_from_yaml_text(cache_yaml)
        self.assertEqual(cfg.rules, [])

    def test_parse_global_config_rejects_invalid_kv_key_prefix_shape(self) -> None:
        cache_yaml = "\n".join(
            [
                "stale_window_ms: 1000",
                "rules:",
                "  - dir_abs: /abs/dir",
                "    cache_mode: read_through",
                "    write_mode: write_through",
                "    kv_key_prefix: /cache",
                "    bytes_field_key: payload",
                "    max_cache_bytes: 16",
                "    on_refresh_error: apply_stale_window",
                "exports: {}",
            ]
        )
        with self.assertRaisesRegex(ValueError, r"must start with '/' and end with '/'"):
            _ = parse_global_config_from_yaml_text(cache_yaml)

    def test_parse_global_config_rejects_empty_export_nodes(self) -> None:
        cache_yaml = "\n".join(
            [
                "stale_window_ms: 1000",
                "rules: []",
                "exports:",
                "  exp1:",
                "    remote_root_dir_abs: /abs/remote",
                "    nodes: []",
                "    cache_max_bytes: 32",
            ]
        )
        with self.assertRaisesRegex(ValueError, r"nodes must be non-empty when provided"):
            _ = parse_global_config_from_yaml_text(cache_yaml)

    def test_parse_global_config_rejects_export_rpc_timeout_ms(self) -> None:
        cache_yaml = "\n".join(
            [
                "stale_window_ms: 1000",
                "rules: []",
                "exports:",
                "  exp1:",
                "    remote_root_dir_abs: /abs/remote",
                "    nodes: [node1]",
                "    cache_max_bytes: 32",
                "    rpc_timeout_ms: 10",
            ]
        )
        with self.assertRaisesRegex(ValueError, r"rpc_timeout_ms is removed"):
            _ = parse_global_config_from_yaml_text(cache_yaml)

    def test_parse_global_config_rejects_export_rpc_paths_field(self) -> None:
        cache_yaml = "\n".join(
            [
                "stale_window_ms: 1000",
                "rules: []",
                "exports:",
                "  exp1:",
                "    remote_root_dir_abs: /abs/remote",
                "    nodes: [node1]",
                "    cache_max_bytes: 32",
                "    rpc_paths:",
                "      stat: /rpc/stat",
            ]
        )
        with self.assertRaisesRegex(ValueError, r"rpc_paths is removed"):
            _ = parse_global_config_from_yaml_text(cache_yaml)

    def test_parse_global_config_rejects_removed_export_routing_mode_field(self) -> None:
        cache_yaml = "\n".join(
            [
                "stale_window_ms: 1000",
                "rules: []",
                "exports:",
                "  exp1:",
                "    remote_root_dir_abs: /abs/remote",
                "    routing_mode: static_nodes",
                "    nodes: [node1]",
                "    cache_max_bytes: 32",
            ]
        )
        with self.assertRaisesRegex(ValueError, r"unknown fields: routing_mode"):
            _ = parse_global_config_from_yaml_text(cache_yaml)

    def test_parse_global_config_rejects_unknown_cache_mode(self) -> None:
        cache_yaml = "\n".join(
            [
                "stale_window_ms: 1000",
                "rules:",
                "  - dir_abs: /abs/dir",
                "    cache_mode: not_a_mode",
                "    write_mode: write_through",
                "    kv_key_prefix: /cache/",
                "    bytes_field_key: payload",
                "    max_cache_bytes: 16",
                "    on_refresh_error: apply_stale_window",
                "exports: {}",
            ]
        )
        with self.assertRaises(ValueError):
            _ = parse_global_config_from_yaml_text(cache_yaml)

    def test_transfer_skip_entry_to_json_obj_preserves_kind_and_relpath(self) -> None:
        obj = transfer_skip_entry_to_json_obj(
            FluxonFsTransferSkipEntry(
                kind=FluxonFsTransferSkipEntryKind.DIR,
                relpath="a/b",
            )
        )
        self.assertEqual(
            obj,
            {
                "kind": "Dir",
                "relpath": "a/b",
            },
        )

    def test_transfer_state_store_to_json_text_encodes_tikv_kind_payload(self) -> None:
        text = transfer_state_store_to_json_text(
            FluxonFsTransferStateStoreConfig(
                kind=FluxonFsTransferStateStoreKind.TIKV,
                tikv=FluxonFsTransferStateStoreTiKvConfig(
                    pd_endpoints=["127.0.0.1:2379"],
                    key_prefix="/fluxon_fs_transfer/",
                ),
            )
        )
        self.assertEqual(
            text,
            (
                '{"kind":{"tikv":{"pd_endpoints":["127.0.0.1:2379"],'
                '"key_prefix":"/fluxon_fs_transfer/"}}}'
            ),
        )

    def test_parse_master_panel_config_defaults_transfer_state_store_kind_to_tikv(self) -> None:
        cfg_path = self._tmp / "cfg_master_panel.yml"
        cfg_path.write_text(
            "\n".join(
                [
                    "fluxon_fs:",
                    "  master_panel:",
                    "    listen_addr: 127.0.0.1:9999",
                    "    public_base_url: http://127.0.0.1:9999",
                    "    auto_refresh_interval_secs: 3",
                    "    access_db_path: ./access.db",
                    "    bootstrap_access_model:",
                    "      users:",
                    "        - username: admin",
                    "          password: admin-pass-123",
                    "          can_manage_users: true",
                    "      scope_access: []",
                    "    transfer_state_store:",
                    "      tikv:",
                    "        pd_endpoints: [127.0.0.1:2379]",
                    "        key_prefix: /fluxon_fs_transfer/",
                ]
            ),
            encoding="utf-8",
        )
        cfg = parse_master_panel_config_from_file(cfg_path)
        self.assertIsInstance(cfg, FluxonFsMasterPanelConfig)
        self.assertEqual(cfg.access_db_path, "./access.db")
        self.assertEqual(cfg.transfer_state_store.kind, FluxonFsTransferStateStoreKind.TIKV)
        self.assertEqual(
            cfg.transfer_state_store.tikv,
            FluxonFsTransferStateStoreTiKvConfig(
                pd_endpoints=["127.0.0.1:2379"],
                key_prefix="/fluxon_fs_transfer/",
            ),
        )

    def test_parse_master_panel_config_rejects_sqlite_transfer_state_store(self) -> None:
        cfg_path = self._tmp / "cfg_master_panel_invalid_sqlite.yml"
        cfg_path.write_text(
            "\n".join(
                [
                    "fluxon_fs:",
                    "  master_panel:",
                    "    listen_addr: 127.0.0.1:9999",
                    "    public_base_url: http://127.0.0.1:9999",
                    "    auto_refresh_interval_secs: 3",
                    "    access_db_path: ./access.db",
                    "    bootstrap_access_model:",
                    "      users:",
                    "        - username: admin",
                    "          password: admin-pass-123",
                    "          can_manage_users: true",
                    "      scope_access: []",
                    "    transfer_state_store:",
                    "      kind: sqlite",
                    "      tikv:",
                    "        pd_endpoints: [127.0.0.1:2379]",
                    "        key_prefix: /fluxon_fs_transfer/",
                ]
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, r"transfer_state_store.kind invalid: sqlite"):
            _ = parse_master_panel_config_from_file(cfg_path)

    def test_parse_master_panel_config_allows_missing_transfer_state_store(self) -> None:
        cfg_path = self._tmp / "cfg_master_panel_without_transfer.yml"
        cfg_path.write_text(
            "\n".join(
                [
                    "fluxon_fs:",
                    "  master_panel:",
                    "    listen_addr: 127.0.0.1:9999",
                    "    public_base_url: http://127.0.0.1:9999",
                    "    auto_refresh_interval_secs: 3",
                    "    access_db_path: ./access.db",
                    "    bootstrap_access_model:",
                    "      users:",
                    "        - username: admin",
                    "          password: admin-pass-123",
                    "          can_manage_users: true",
                    "      scope_access: []",
                ]
            ),
            encoding="utf-8",
        )
        cfg = parse_master_panel_config_from_file(cfg_path)
        self.assertIsInstance(cfg, FluxonFsMasterPanelConfig)
        self.assertEqual(cfg.access_db_path, "./access.db")
        self.assertIsNone(cfg.transfer_state_store)
        self.assertEqual(
            cfg.bootstrap_access_model,
            FluxonFsAccessModel(
                users=[
                    FluxonFsAccessUser(
                        username="admin",
                        password="admin-pass-123",
                        can_manage_users=True,
                    )
                ],
                scope_access=[],
            ),
        )

    def test_parse_master_panel_config_requires_bootstrap_access_model(self) -> None:
        cfg_path = self._tmp / "cfg_master_panel_missing_bootstrap.yml"
        cfg_path.write_text(
            "\n".join(
                [
                    "fluxon_fs:",
                    "  master_panel:",
                    "    listen_addr: 127.0.0.1:9999",
                    "    public_base_url: http://127.0.0.1:9999",
                    "    auto_refresh_interval_secs: 3",
                    "    access_db_path: ./access.db",
                ]
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            ValueError,
            r"fluxon_fs.master_panel.bootstrap_access_model is required",
        ):
            _ = parse_master_panel_config_from_file(cfg_path)

    def test_parse_master_panel_config_accepts_bootstrap_access_model(self) -> None:
        cfg_path = self._tmp / "cfg_master_panel_with_bootstrap_access_model.yml"
        cfg_path.write_text(
            "\n".join(
                [
                    "fluxon_fs:",
                    "  master_panel:",
                    "    listen_addr: 127.0.0.1:9999",
                    "    public_base_url: http://127.0.0.1:9999",
                    "    auto_refresh_interval_secs: 3",
                    "    access_db_path: ./access.db",
                    "    bootstrap_access_model:",
                    "      users:",
                    "        - username: admin",
                    "          password: admin-pass-123",
                    "          can_manage_users: true",
                    "      scope_access:",
                    "        - export_name: demo",
                    "          prefix: ''",
                    "          mode: read_write",
                    "          usernames: [admin]",
                ]
            ),
            encoding="utf-8",
        )
        cfg = parse_master_panel_config_from_file(cfg_path)
        self.assertIsInstance(cfg, FluxonFsMasterPanelConfig)
        self.assertEqual(
            cfg.bootstrap_access_model,
            FluxonFsAccessModel(
                users=[
                    FluxonFsAccessUser(
                        username="admin",
                        password="admin-pass-123",
                        can_manage_users=True,
                    )
                ],
                scope_access=[
                    FluxonFsScopeAccess(
                        export_name="demo",
                        prefix="",
                        mode=FluxonFsScopeAccessMode.READ_WRITE,
                        usernames=["admin"],
                    )
                ],
            ),
        )


if __name__ == "__main__":
    main()
