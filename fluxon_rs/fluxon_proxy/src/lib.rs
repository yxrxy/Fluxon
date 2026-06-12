use anyhow::Context;
use prost::bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use fluxon_kv::cluster_manager::NodeID;
use fluxon_kv::p2p::msg_pack::{MsgPack, RPCCaller};
use fluxon_kv::p2p::p2p_module::{
    P2pModule, UserRpcAsyncHandler, UserRpcReq, UserRpcResp, user_rpc_register_handler_async,
};
use fluxon_kv::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, OK};

pub use fluxon_proxy_proto::{
    HeaderKv, PANEL_PROXY_USER_RPC_PATH_V1, PanelProxyMethod, PanelProxyReq, PanelProxyResp,
};

pub type PanelProxyHandlerFuture =
    Pin<Box<dyn Future<Output = anyhow::Result<PanelProxyResp>> + Send>>;

pub type PanelProxyHandler = Arc<dyn Fn(PanelProxyReq) -> PanelProxyHandlerFuture + Send + Sync>;

pub fn ensure_panel_proxy_userrpc_client_registered(p2p: &P2pModule) {
    RPCCaller::<UserRpcReq>::new().regist(p2p);
}

pub async fn call_panel_proxy_via_userrpc(
    p2p: &P2pModule,
    node_id: NodeID,
    req: PanelProxyReq,
    timeout: Duration,
) -> anyhow::Result<PanelProxyResp> {
    let payload = bitcode::encode(&req);
    let msg = MsgPack {
        serialize_part: UserRpcReq {
            path: PANEL_PROXY_USER_RPC_PATH_V1.to_string(),
        },
        raw_bytes: vec![Bytes::from(payload)],
    };

    let caller = RPCCaller::<UserRpcReq>::new();
    let resp_pack: MsgPack<UserRpcResp> = caller
        .call(p2p, node_id, msg, Some(timeout), 0)
        .await
        .map_err(|e| anyhow::anyhow!("panel proxy userrpc call failed: {}", e))?;

    if resp_pack.serialize_part.error_code != OK {
        let err = KvError::from_json(
            resp_pack.serialize_part.error_code,
            &resp_pack.serialize_part.error_json,
        );
        return Err(anyhow::anyhow!(
            "panel proxy userrpc upstream error: {}",
            err
        ));
    }

    let Some(bytes) = resp_pack.raw_bytes.get(0).cloned() else {
        let err = ApiError::UserRpcMissingPayload {
            path: PANEL_PROXY_USER_RPC_PATH_V1.to_string(),
        };
        return Err(anyhow::anyhow!(
            "panel proxy userrpc response invalid: {}",
            err
        ));
    };

    let resp: PanelProxyResp = bitcode::decode(bytes.as_ref())
        .map_err(|e| anyhow::anyhow!("panel proxy decode resp failed: {}", e))?;
    Ok(resp)
}

pub fn register_panel_proxy_handler_on_userrpc(p2p: &P2pModule, handler: PanelProxyHandler) {
    user_rpc_register_handler_async(
        p2p,
        PANEL_PROXY_USER_RPC_PATH_V1.to_string(),
        Arc::new(PanelProxyUserRpcHandler { handler }),
    );
}

struct PanelProxyUserRpcHandler {
    handler: PanelProxyHandler,
}

impl UserRpcAsyncHandler for PanelProxyUserRpcHandler {
    fn handle(
        &self,
        _from_node: NodeID,
        payload: Vec<u8>,
    ) -> fluxon_kv::p2p::p2p_module::UserRpcFuture {
        let handler = self.handler.clone();
        Box::pin(async move {
            let req: PanelProxyReq = bitcode::decode(&payload).map_err(|e| {
                KvError::Api(ApiError::InvalidArgument {
                    detail: format!("panel proxy decode req failed: {}", e),
                })
            })?;
            let resp = (handler)(req).await.map_err(|e| {
                KvError::Api(ApiError::Unknown {
                    detail: format!("panel proxy handler failed: {}", e),
                })
            })?;
            Ok(bitcode::encode(&resp))
        })
    }
}

pub fn build_fluxon_cli_registered_panel_proxy_backend(
    fw: Arc<fluxon_kv::Framework>,
    timeout: Duration,
) -> fluxon_cli::server::RegisteredPanelProxyBackend {
    ensure_panel_proxy_userrpc_client_registered(fw.p2p_view().p2p_module());

    Arc::new(move |req| {
        let fw = fw.clone();
        Box::pin(async move {
            let method = match req.method.as_str() {
                "GET" => PanelProxyMethod::Get,
                "HEAD" => PanelProxyMethod::Head,
                "POST" => PanelProxyMethod::Post,
                "PUT" => PanelProxyMethod::Put,
                "DELETE" => PanelProxyMethod::Delete,
                "OPTIONS" => PanelProxyMethod::Options,
                "PATCH" => PanelProxyMethod::Patch,
                _ => anyhow::bail!("unsupported proxy method: {}", req.method),
            };

            let headers: Vec<HeaderKv> = req
                .headers
                .into_iter()
                .map(|(k, v)| HeaderKv { k, v })
                .collect();

            let msg = PanelProxyReq {
                method,
                path_and_query: req.path_and_query,
                headers,
                body: req.body,
            };

            let node_id: NodeID = std::borrow::Cow::Owned(req.node_id);
            let resp =
                call_panel_proxy_via_userrpc(fw.p2p_view().p2p_module(), node_id, msg, timeout)
                    .await
                    .with_context(|| "panel proxy userrpc failed")?;

            Ok::<fluxon_cli::server::RegisteredPanelProxyBackendResp, anyhow::Error>(
                fluxon_cli::server::RegisteredPanelProxyBackendResp {
                    status: resp.status,
                    headers: resp.headers.into_iter().map(|kv| (kv.k, kv.v)).collect(),
                    body: resp.body,
                },
            )
        })
    })
}
