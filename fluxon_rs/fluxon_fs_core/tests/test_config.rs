use fluxon_fs_core::config::{
    FS_EXPORT_CACHE_BYTES_FIELD_KEY, FluxonFsConfigError, FluxonFsExportRoutingMode,
    FluxonFsMasterConfigError, export_cache_kv_key_prefix_for_export_name_v1,
    export_rpc_paths_for_export_name_v1, parse_cache_config_yaml,
    parse_master_config_from_yaml_text,
};

fn minimal_cache_yaml() -> String {
    // This YAML is exactly the content of `fluxon_fs.cache` (not the whole config file).
    // The schema uses `serde(deny_unknown_fields)`, so keep it minimal and explicit.
    [
        "stale_window_ms: 1000",
        "rules: []",
        "exports:",
        "  exp1:",
        "    remote_root_dir_abs: /abs/remote_root",
        "    nodes: [node1]",
        "    cache_max_bytes: 1048576",
    ]
    .join("\n")
}

fn rule_yaml(kv_key_prefix: &str) -> String {
    [
        "stale_window_ms: 1000",
        "rules:",
        "  - dir_abs: /abs/cache",
        "    cache_mode: read_through",
        "    write_mode: write_through",
        &format!("    kv_key_prefix: {}", kv_key_prefix),
        "    bytes_field_key: bytes",
        "    max_cache_bytes: 16",
        "    on_refresh_error: apply_stale_window",
        "exports: {}",
    ]
    .join("\n")
}

#[test]
fn parse_cache_config_yaml_accepts_minimal_valid() {
    let cfg = parse_cache_config_yaml(&minimal_cache_yaml()).unwrap();
    assert_eq!(cfg.stale_window_ms, 1000);
    assert_eq!(cfg.rules.len(), 0);
    assert!(cfg.exports.contains_key("exp1"));
    let exp = cfg.exports.get("exp1").unwrap();
    assert_eq!(exp.routing_mode, FluxonFsExportRoutingMode::StaticNodes);
    assert_eq!(exp.nodes, vec!["node1".to_string()]);
    assert_eq!(
        exp.cache_kv_key_prefix,
        export_cache_kv_key_prefix_for_export_name_v1("exp1")
    );
    assert_eq!(exp.cache_bytes_field_key, FS_EXPORT_CACHE_BYTES_FIELD_KEY);
    assert_eq!(
        exp.rpc_paths.stat,
        export_rpc_paths_for_export_name_v1("exp1").stat
    );
}

#[test]
fn parse_cache_config_yaml_rejects_zero_stale_window() {
    let text = minimal_cache_yaml().replace("stale_window_ms: 1000", "stale_window_ms: 0");
    let err = parse_cache_config_yaml(&text).unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("stale_window_ms must be > 0"));
}

#[test]
fn parse_cache_config_yaml_rejects_invalid_kv_prefix_shape() {
    let text = rule_yaml("cache");
    let err = parse_cache_config_yaml(&text).unwrap_err();
    assert!(matches!(err, FluxonFsConfigError::Invalid { .. }));
    let msg = format!("{}", err);
    assert!(msg.contains("kv_key_prefix"));
}

#[test]
fn parse_cache_config_yaml_rejects_empty_export_nodes() {
    let text = minimal_cache_yaml().replace("nodes: [node1]", "nodes: []");
    let err = parse_cache_config_yaml(&text).unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("nodes must be non-empty when provided"));
}

#[test]
fn parse_cache_config_yaml_rejects_unknown_fields() {
    let text = minimal_cache_yaml().replace(
        "cache_max_bytes: 1048576",
        "cache_max_bytes: 1048576\n    unexpected_field: 1",
    );
    let err = parse_cache_config_yaml(&text).unwrap_err();
    assert!(matches!(err, FluxonFsConfigError::Invalid { .. }));
    let msg = format!("{}", err);
    assert!(msg.contains("yaml parse failed"));
}

#[test]
fn parse_cache_config_yaml_accepts_omitted_rules() {
    let text = minimal_cache_yaml().replace("rules: []\n", "");
    let cfg = parse_cache_config_yaml(&text).unwrap();
    assert!(cfg.rules.is_empty());
}

#[test]
fn parse_cache_config_yaml_derives_agent_registry_when_nodes_omitted() {
    let text = minimal_cache_yaml().replace("    nodes: [node1]\n", "");
    let cfg = parse_cache_config_yaml(&text).unwrap();
    let exp = cfg.exports.get("exp1").unwrap();
    assert_eq!(exp.routing_mode, FluxonFsExportRoutingMode::AgentRegistry);
    assert!(exp.nodes.is_empty());
}

#[test]
fn parse_master_config_rejects_removed_rpc_timeout_ms() {
    let text = [
        "fluxon_fs:",
        "  master:",
        "    instance_key: master",
        "    pull_interval_ms: 1000",
        "    rpc_timeout_ms: 10",
    ]
    .join("\n");
    let err = parse_master_config_from_yaml_text(&text).unwrap_err();
    assert!(matches!(err, FluxonFsMasterConfigError::Invalid { .. }));
    let msg = format!("{}", err);
    assert!(msg.contains("rpc_timeout_ms is removed"));
}

#[test]
fn parse_master_config_accepts_instance_key_only() {
    let text = [
        "kvclient:",
        "  instance_key: ignored_by_parser",
        "fluxon_fs:",
        "  master:",
        "    instance_key: master",
    ]
    .join("\n");
    let cfg = parse_master_config_from_yaml_text(&text).unwrap();
    assert_eq!(cfg.instance_key, "master");
    assert_eq!(cfg.pull_interval_ms, None);
}

#[test]
fn parse_master_config_accepts_pull_interval_ms() {
    let text = [
        "fluxon_fs:",
        "  master:",
        "    instance_key: master",
        "    pull_interval_ms: 1000",
    ]
    .join("\n");
    let cfg = parse_master_config_from_yaml_text(&text).unwrap();
    assert_eq!(cfg.instance_key, "master");
    assert_eq!(cfg.pull_interval_ms, Some(1000));
}

#[test]
fn parse_master_config_rejects_missing_fluxon_fs() {
    let text = ["kvclient:", "  instance_key: x"].join("\n");
    let err = parse_master_config_from_yaml_text(&text).unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("fluxon_fs is required"));
}

#[test]
fn parse_master_config_rejects_missing_master_section() {
    let text = ["fluxon_fs:", "  cache: {}"].join("\n");
    let err = parse_master_config_from_yaml_text(&text).unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("fluxon_fs.master is required"));
}

#[test]
fn parse_master_config_rejects_legacy_rpc_section() {
    let text = [
        "fluxon_fs:",
        "  rpc:",
        "    master_instance_key: master",
        "    pull_interval_ms: 1000",
    ]
    .join("\n");
    let err = parse_master_config_from_yaml_text(&text).unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("fluxon_fs.rpc is removed"));
}
