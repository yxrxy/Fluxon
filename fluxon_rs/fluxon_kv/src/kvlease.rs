use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::master_lease_manager::msg_pack::{AllocateClientLeaseReq, ClientLeaseKeepaliveReq};
use crate::p2p::msg_pack::{MsgPack, RPCCaller};
use crate::p2p::p2p_module::P2pModule;
use crate::rpcresp_kvresult_convert;
use crate::rpcresp_kvresult_convert::msg_and_error::{KvError, KvResult};
use std::time::Duration;

const LEASE_RPC_TIMEOUT_SECS: u64 = 10;
// Must be >= MIN_EXPLICIT_RPC_TIMEOUT_SECS; otherwise P2P rejects the call before sending.
// (first-access gating moved to P2P layer; keep kvlease focused on lease RPC)

/// Register lease-related RPC callers for any module that has P2P access.
/// Call from owner(external/client) module init2 to avoid scattered registrations.
pub fn init_for_p2p_owner(p2p: &P2pModule) {
    RPCCaller::<AllocateClientLeaseReq>::new().regist(p2p);
    RPCCaller::<ClientLeaseKeepaliveReq>::new().regist(p2p);
}

/// Allocate a client lease via master using generic views (P2P + ClusterManager).
/// Both owner/client and external roles should go through this helper to avoid
/// duplicating the RPC logic.
pub async fn allocate_lease(
    p2p: &P2pModule,
    cm: &crate::cluster_manager::ClusterManager,
    ttl_seconds: u64,
) -> KvResult<u64> {
    let master_node_id = cm.find_or_wait_master_node().await?;
    // Per-peer first-access gating is handled inside P2P layer call path.
    let req = MsgPack {
        serialize_part: AllocateClientLeaseReq {
            requested_ttl_seconds: ttl_seconds,
        },
        raw_bytes: Vec::new(),
    };
    let caller = RPCCaller::<AllocateClientLeaseReq>::new();
    let resp = caller
        .call(
            p2p,
            master_node_id.into(),
            req,
            Some(Duration::from_secs(LEASE_RPC_TIMEOUT_SECS)),
            usize::MAX,
        )
        .await
        .map_err(KvError::from)?;

    rpcresp_kvresult_convert::try_from_code(
        resp.serialize_part.error_code,
        resp.serialize_part.error_json.clone(),
    )?;
    Ok(resp.serialize_part.lease_id)
}

/// Keepalive a client lease via master using generic views (P2P + ClusterManager).
pub async fn keepalive_lease(
    p2p: &P2pModule,
    cm: &crate::cluster_manager::ClusterManager,
    lease_id: u64,
) -> KvResult<()> {
    let master_node_id = cm.find_or_wait_master_node().await?;
    // Per-peer first-access gating is handled inside P2P layer call path.
    let req = MsgPack {
        serialize_part: ClientLeaseKeepaliveReq {
            lease_id,
            custom_ttl: 0,
        },
        raw_bytes: Vec::new(),
    };

    let caller = RPCCaller::<ClientLeaseKeepaliveReq>::new();
    let resp = caller
        .call(
            p2p,
            master_node_id.into(),
            req,
            Some(Duration::from_secs(LEASE_RPC_TIMEOUT_SECS)),
            0,
        )
        .await
        .map_err(KvError::from)?;

    rpcresp_kvresult_convert::try_from_code(
        resp.serialize_part.error_code,
        resp.serialize_part.error_json.clone(),
    )?;
    Ok(())
}
