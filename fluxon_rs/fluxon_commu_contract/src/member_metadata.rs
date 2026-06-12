use bitcode::{Decode, Encode};
pub use fluxon_commu_contract::{RdmaLinkLayer, RdmaPhysState, RdmaPortSnapshot, RdmaPortState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::warn;

pub const META_KEY_ACCESSIBLE_IP: &str = "accessible_ip";
pub const META_KEY_HOSTNAME: &str = "hostname";
pub const META_KEY_LOCAL_IPC_ROOT: &str = "local_ipc_root";
pub const META_KEY_PRODUCT_UUID: &str = "product_uuid";
pub const META_KEY_PID: &str = "pid";
pub const META_KEY_CMD: &str = "cmd";
pub const META_KEY_SHARED_STORAGE_NODE_ID: &str = "shared_storage_node_id";
pub const META_KEY_SHARED_STORAGE_NODE_START_TIME: &str = "shared_storage_node_start_time";
pub const META_KEY_RDMA_CONTROL: &str = "rdma_control";
pub const META_KEY_RDMA_RUNTIME: &str = "rdma_runtime";
pub const ETCD_PREFIX_CLUSTER_MEMBER_BASE: &str = "/fluxon_commu_member_base";
pub const ETCD_PREFIX_CLUSTER_MEMBER_EXT: &str = "/fluxon_commu_member_ext";
pub const ETCD_PREFIX_CLUSTER_RDMA_CONTROL: &str = "/fluxon_commu_rdma_control";

// Share-group is now a commu-native object and carries owner generation in the keyspace.
const ETCD_PREFIX_SHARE_GROUP: &str = "/fluxon_commu_share_group";
pub const SHARE_GROUP_MEMBER_VALUE: &str = "1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Encode, Decode)]
pub struct AccessibleIpInfo {
    pub ip: String,
    pub node_start_time: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Encode, Decode)]
pub struct MemberRdmaControl {
    pub node_start_time: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_product_uuid: Option<String>,
    // Control-plane authority is device-scoped. This list must contain bare
    // RDMA device names such as `mlx5_0`, never runtime port keys such as
    // `mlx5_0:1`.
    pub enabled_devices: Vec<String>,
}

impl MemberRdmaControl {
    fn machine_metadata_value(metadata: &HashMap<String, String>, key: &str) -> Option<String> {
        let value = metadata.get(key)?.trim();
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    }

    pub fn machine_hostname_from_metadata(metadata: &HashMap<String, String>) -> Option<String> {
        Self::machine_metadata_value(metadata, META_KEY_HOSTNAME)
    }

    pub fn machine_product_uuid_from_metadata(
        metadata: &HashMap<String, String>,
    ) -> Option<String> {
        Self::machine_metadata_value(metadata, META_KEY_PRODUCT_UUID)
    }

    pub fn machine_identity_conflicts_with_metadata(
        &self,
        metadata: &HashMap<String, String>,
    ) -> bool {
        let current_product_uuid = Self::machine_product_uuid_from_metadata(metadata);
        if let (Some(saved), Some(current)) = (
            self.machine_product_uuid.as_deref(),
            current_product_uuid.as_deref(),
        ) {
            return saved != current;
        }

        let current_hostname = Self::machine_hostname_from_metadata(metadata);
        if let (Some(saved), Some(current)) = (
            self.machine_hostname.as_deref(),
            current_hostname.as_deref(),
        ) {
            return saved != current;
        }

        false
    }

    pub fn needs_machine_identity_refresh(&self, metadata: &HashMap<String, String>) -> bool {
        self.machine_hostname != Self::machine_hostname_from_metadata(metadata)
            || self.machine_product_uuid != Self::machine_product_uuid_from_metadata(metadata)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Encode, Decode)]
#[serde(rename_all = "snake_case")]
pub enum MemberRdmaTransferEngineState {
    Disabled,
    Starting,
    Running,
    Restarting,
    Stopped,
}

impl MemberRdmaTransferEngineState {
    pub fn as_str(self) -> &'static str {
        match self {
            MemberRdmaTransferEngineState::Disabled => "disabled",
            MemberRdmaTransferEngineState::Starting => "starting",
            MemberRdmaTransferEngineState::Running => "running",
            MemberRdmaTransferEngineState::Restarting => "restarting",
            MemberRdmaTransferEngineState::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Encode, Decode)]
pub struct MemberRdmaTransferEngineRuntime {
    pub state: MemberRdmaTransferEngineState,
    pub consecutive_start_failures: u64,
}

impl MemberRdmaTransferEngineRuntime {
    pub fn new(state: MemberRdmaTransferEngineState) -> Self {
        Self {
            state,
            consecutive_start_failures: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberRdmaRuntime {
    pub node_start_time: i64,
    pub probe_error: Option<String>,
    pub ports: Vec<RdmaPortSnapshot>,
    pub desired_enabled_ports: Vec<String>,
    pub effective_enabled_ports: Vec<String>,
    pub transfer_engine: MemberRdmaTransferEngineRuntime,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct MemberRdmaResolvedConfig {
    pub node_start_time: i64,
    pub ports: Vec<RdmaPortSnapshot>,
    // Control-plane authority list after normalization.
    pub enabled_devices: Vec<String>,
    // Device-scoped subset that remains usable after intersecting runtime probe state.
    pub effective_enabled_devices: Vec<String>,
    // Runtime/probe-derived port keys that correspond to the device authority list.
    pub desired_enabled_ports: Vec<String>,
    pub effective_enabled_ports: Vec<String>,
}

impl MemberRdmaResolvedConfig {
    pub fn new_empty(node_start_time: i64) -> Self {
        Self {
            node_start_time,
            ports: Vec::new(),
            enabled_devices: Vec::new(),
            effective_enabled_devices: Vec::new(),
            desired_enabled_ports: Vec::new(),
            effective_enabled_ports: Vec::new(),
        }
    }
}

pub fn validate_rdma_control_enabled_devices(enabled_devices: &[String]) -> Result<(), String> {
    for device in enabled_devices {
        let trimmed = device.trim();
        if trimmed.contains(':') {
            return Err(format!(
                "rdma control enabled_devices expects bare device names like `mlx5_0`, got port-scoped entry `{}`; keep port resolution in runtime probe state instead of feeding port keys into control-plane authority",
                trimmed
            ));
        }
    }
    Ok(())
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Encode, Decode,
)]
pub struct ShareGroupOwnerRef {
    pub owner_id: String,
    pub owner_start_time: i64,
}

pub fn share_group_owner_ref_from_metadata(
    metadata: &HashMap<String, String>,
) -> Option<ShareGroupOwnerRef> {
    let owner_id = metadata.get(META_KEY_SHARED_STORAGE_NODE_ID)?.clone();
    let owner_start_time_text = metadata.get(META_KEY_SHARED_STORAGE_NODE_START_TIME)?;
    let owner_start_time = match owner_start_time_text.parse::<i64>() {
        Ok(value) => value,
        Err(err) => {
            warn!(
                "Invalid share-group owner_start_time in metadata: key={} value={} err={}",
                META_KEY_SHARED_STORAGE_NODE_START_TIME, owner_start_time_text, err
            );
            return None;
        }
    };
    Some(ShareGroupOwnerRef {
        owner_id,
        owner_start_time,
    })
}

pub fn share_group_member_key(
    cluster_name: &str,
    owner_id: &str,
    owner_start_time: i64,
    member_id: &str,
) -> String {
    format!(
        "{}/{}/owner/{}/start_time/{}/members/{}",
        ETCD_PREFIX_SHARE_GROUP, cluster_name, owner_id, owner_start_time, member_id
    )
}

pub fn cluster_member_base_prefix(cluster_name: &str) -> String {
    format!(
        "{}/{}/members",
        ETCD_PREFIX_CLUSTER_MEMBER_BASE, cluster_name
    )
}

pub fn cluster_member_base_key(cluster_name: &str, member_id: &str) -> String {
    format!("{}/{}", cluster_member_base_prefix(cluster_name), member_id)
}

pub fn cluster_member_ext_prefix(cluster_name: &str) -> String {
    format!(
        "{}/{}/members",
        ETCD_PREFIX_CLUSTER_MEMBER_EXT, cluster_name
    )
}

pub fn cluster_member_ext_key(cluster_name: &str, member_id: &str, metadata_key: &str) -> String {
    format!(
        "{}/{}/{}",
        cluster_member_ext_prefix(cluster_name),
        member_id,
        metadata_key
    )
}

pub fn cluster_owner_rdma_control_prefix(cluster_name: &str) -> String {
    format!(
        "{}/{}/owners",
        ETCD_PREFIX_CLUSTER_RDMA_CONTROL, cluster_name
    )
}

pub fn cluster_owner_rdma_control_key(cluster_name: &str, member_id: &str) -> String {
    format!(
        "{}/{}",
        cluster_owner_rdma_control_prefix(cluster_name),
        member_id
    )
}

#[cfg(test)]
mod tests {
    use super::{
        META_KEY_HOSTNAME, META_KEY_PRODUCT_UUID, MemberRdmaControl,
        cluster_owner_rdma_control_key, cluster_owner_rdma_control_prefix,
        validate_rdma_control_enabled_devices,
    };
    use std::collections::HashMap;

    #[test]
    fn test_cluster_owner_rdma_control_key_is_cluster_scoped() {
        assert_eq!(
            cluster_owner_rdma_control_prefix("cluster-a"),
            "/fluxon_commu_rdma_control/cluster-a/owners"
        );
        assert_eq!(
            cluster_owner_rdma_control_key("cluster-a", "owner_node-1"),
            "/fluxon_commu_rdma_control/cluster-a/owners/owner_node-1"
        );
        assert_eq!(
            cluster_owner_rdma_control_key("cluster-b", "owner_node-1"),
            "/fluxon_commu_rdma_control/cluster-b/owners/owner_node-1"
        );
    }

    #[test]
    fn test_member_rdma_control_machine_identity_conflict_prefers_product_uuid() {
        let mut metadata = HashMap::new();
        metadata.insert(META_KEY_HOSTNAME.to_string(), "host-b".to_string());
        metadata.insert(META_KEY_PRODUCT_UUID.to_string(), "uuid-a".to_string());
        let control = MemberRdmaControl {
            node_start_time: 1,
            machine_hostname: Some("host-a".to_string()),
            machine_product_uuid: Some("uuid-a".to_string()),
            enabled_devices: vec!["mlx5_0".to_string()],
        };
        assert!(!control.machine_identity_conflicts_with_metadata(&metadata));
        assert!(control.needs_machine_identity_refresh(&metadata));
    }

    #[test]
    fn test_member_rdma_control_machine_identity_conflict_falls_back_to_hostname() {
        let mut metadata = HashMap::new();
        metadata.insert(META_KEY_HOSTNAME.to_string(), "host-b".to_string());
        let control = MemberRdmaControl {
            node_start_time: 1,
            machine_hostname: Some("host-a".to_string()),
            machine_product_uuid: None,
            enabled_devices: vec!["mlx5_0".to_string()],
        };
        assert!(control.machine_identity_conflicts_with_metadata(&metadata));
    }

    #[test]
    fn test_member_rdma_control_machine_identity_missing_fields_need_refresh() {
        let mut metadata = HashMap::new();
        metadata.insert(META_KEY_HOSTNAME.to_string(), "host-a".to_string());
        metadata.insert(META_KEY_PRODUCT_UUID.to_string(), "uuid-a".to_string());
        let control = MemberRdmaControl {
            node_start_time: 1,
            machine_hostname: None,
            machine_product_uuid: None,
            enabled_devices: vec!["mlx5_0".to_string()],
        };
        assert!(!control.machine_identity_conflicts_with_metadata(&metadata));
        assert!(control.needs_machine_identity_refresh(&metadata));
    }

    #[test]
    fn test_validate_rdma_control_enabled_devices_accepts_bare_devices() {
        let devices = vec!["mlx5_0".to_string(), "mlx5_1".to_string()];
        validate_rdma_control_enabled_devices(&devices).unwrap();
    }

    #[test]
    fn test_validate_rdma_control_enabled_devices_rejects_port_keys() {
        let devices = vec!["mlx5_0:1".to_string()];
        let err = validate_rdma_control_enabled_devices(&devices).unwrap_err();
        assert!(err.contains("bare device names"));
        assert!(err.contains("mlx5_0:1"));
    }
}
