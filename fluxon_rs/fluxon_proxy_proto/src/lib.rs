use bitcode::{Decode, Encode};

// Stable UserRpc path used for panel proxy requests.
//
// English note:
// - This constant is part of the protocol contract. It must not be duplicated as ad-hoc strings
//   across services; otherwise different publishers will register different paths and proxy calls
//   will fail non-deterministically.
pub const PANEL_PROXY_USER_RPC_PATH_V1: &str = "/fluxon/panel_proxy/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum PanelProxyMethod {
    Get,
    Head,
    Post,
    Put,
    Delete,
    Options,
    Patch,
}

impl Default for PanelProxyMethod {
    fn default() -> Self {
        // English note: required by derive(Default) on request structs so they can satisfy
        // generic trait bounds in transport glue. This is not a runtime "fallback method".
        PanelProxyMethod::Get
    }
}

#[derive(Debug, Clone, Default, Encode, Decode)]
pub struct HeaderKv {
    pub k: String,
    pub v: String,
}

#[derive(Debug, Clone, Default, Encode, Decode)]
pub struct PanelProxyReq {
    pub method: PanelProxyMethod,
    pub path_and_query: String,
    pub headers: Vec<HeaderKv>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, Default, Encode, Decode)]
pub struct PanelProxyResp {
    pub status: u16,
    pub headers: Vec<HeaderKv>,
    pub body: Vec<u8>,
}
