use crate::rpcresp_kvresult_convert::msg_and_error::MsgId;
use bitcode::{Decode, Encode};

// --- RPC for Client Lease Management ---

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct AllocateClientLeaseReq {
    /// Requested TTL seconds for the client lease.
    ///
    /// Rules:
    /// - `requested_ttl_seconds` must be >= MasterLeaseManager::MIN_CLIENT_TTL_SECONDS
    ///   (currently 90s) on the server side, otherwise InvalidTTL is returned.
    /// - The value 0 is reserved as an invalid TTL and MUST NOT be used by clients
    ///   to mean "use default"; default TTL (if any) is a purely server-side concern.
    pub requested_ttl_seconds: u64,
}
impl MsgPackSerializePart for AllocateClientLeaseReq {
    fn msg_id(&self) -> u32 {
        MsgId::AllocateClientLeaseReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct AllocateClientLeaseResp {
    pub error_code: crate::rpcresp_kvresult_convert::msg_and_error::ErrorCode,
    pub error_json: String,
    pub lease_id: u64,
    pub ttl_seconds: u64,
}
impl MsgPackSerializePart for AllocateClientLeaseResp {
    fn msg_id(&self) -> u32 {
        MsgId::AllocateClientLeaseResp as u32
    }
}
impl RPCReq for AllocateClientLeaseReq {
    type Resp = AllocateClientLeaseResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ClientLeaseKeepaliveReq {
    pub lease_id: u64,
    /// Optional custom TTL in seconds encoded as:
    ///   - 0: no custom TTL override (server uses the lease's own TTL)
    ///   - x>0: one-shot custom TTL override for this keepalive
    ///
    /// Client APIs MUST NOT expose "0 means default" as a public semantic; they
    /// should model this as `Option<u64>` and never let callers pass 0 directly.
    pub custom_ttl: u64,
}
impl MsgPackSerializePart for ClientLeaseKeepaliveReq {
    fn msg_id(&self) -> u32 {
        MsgId::ClientLeaseKeepaliveReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ClientLeaseKeepaliveResp {
    pub error_code: crate::rpcresp_kvresult_convert::msg_and_error::ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for ClientLeaseKeepaliveResp {
    fn msg_id(&self) -> u32 {
        MsgId::ClientLeaseKeepaliveResp as u32
    }
}
impl RPCReq for ClientLeaseKeepaliveReq {
    type Resp = ClientLeaseKeepaliveResp;
}

// Import the necessary traits
use crate::p2p::msg_pack::{MsgPackSerializePart, RPCReq};
