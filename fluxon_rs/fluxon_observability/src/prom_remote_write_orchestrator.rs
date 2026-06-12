use anyhow::Context;
use async_trait::async_trait;
use bitcode::{Decode, Encode};
use prost::bytes::Bytes;
use std::time::Duration;
use tracing::{debug, warn};

use fluxon_util::prom_remote_write::{
    CONTENT_TYPE, HEADER_NAME_REMOTE_WRITE_VERSION, REMOTE_WRITE_VERSION_01,
};

pub const PROM_REMOTE_WRITE_PROXY_REQ_MSG_ID: u32 = 4201;
pub const PROM_REMOTE_WRITE_PROXY_RESP_MSG_ID: u32 = 4202;

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PromRemoteWriteProxyReq {
    /// Candidate remote-write endpoints in priority order.
    pub remote_write_urls: Vec<String>,
    /// Timeout applied for each HTTP POST attempt on the proxy node.
    pub per_url_timeout_ms: u64,
    /// User-Agent to preserve the sender identity for upstream logs.
    pub user_agent: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PromRemoteWriteProxyResp {
    pub ok: bool,
    /// Endpoint that succeeded on the proxy node (if any).
    pub selected_url: Option<String>,
    /// Human-readable error detail for debugging/logging.
    pub detail: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PromRemoteWriteSendPath {
    Direct,
    Proxy,
}

#[derive(Clone, Debug)]
pub enum PromRemoteWriteAttemptResult<N> {
    Disabled,
    Sent {
        path: PromRemoteWriteSendPath,
        selected_url: Option<String>,
        proxy_node: Option<N>,
    },
    SkippedNoProxy {
        detail: String,
    },
    ProxyFailed {
        proxy_node: N,
        detail: String,
    },
}

#[async_trait]
pub trait PromRemoteWriteDirectSender: Send + Sync {
    async fn send(&self, url: &str, user_agent: &str, payload: Bytes) -> anyhow::Result<()>;
}

#[derive(Clone, Debug)]
pub struct PromRemoteWriteHttpSender {
    http: reqwest::Client,
}

impl PromRemoteWriteHttpSender {
    pub fn new(http: reqwest::Client) -> Self {
        Self { http }
    }
}

#[async_trait]
impl PromRemoteWriteDirectSender for PromRemoteWriteHttpSender {
    async fn send(&self, url: &str, user_agent: &str, payload: Bytes) -> anyhow::Result<()> {
        let http_request = self
            .http
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, CONTENT_TYPE)
            .header(HEADER_NAME_REMOTE_WRITE_VERSION, REMOTE_WRITE_VERSION_01)
            .header(reqwest::header::CONTENT_ENCODING, "snappy")
            .header(reqwest::header::USER_AGENT, user_agent)
            .body(payload)
            .build()
            .with_context(|| format!("build remote-write request: url={}", url))?;

        let response = self
            .http
            .execute(http_request)
            .await
            .with_context(|| format!("execute remote-write request: url={}", url))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response.text().await.unwrap_or_else(|_| "".to_string());
        anyhow::bail!("remote-write http {}: {}", status.as_u16(), body);
    }
}

pub trait PromRemoteWriteProxyPicker<N>: Send + Sync {
    fn pick_proxy_node(&self) -> Option<N>;
}

#[async_trait]
pub trait PromRemoteWriteProxyCaller<N>: Send + Sync {
    async fn call_proxy(
        &self,
        proxy_node: N,
        req: PromRemoteWriteProxyReq,
        payload: Bytes,
        rpc_timeout: Duration,
    ) -> anyhow::Result<PromRemoteWriteProxyResp>;
}

fn order_urls(remote_write_urls: &[String], selected_url_opt: Option<&str>) -> Vec<String> {
    let mut ordered = Vec::with_capacity(remote_write_urls.len());
    if let Some(sel) = selected_url_opt {
        if remote_write_urls.iter().any(|u| u == sel) {
            ordered.push(sel.to_string());
        }
    }
    for u in remote_write_urls {
        if ordered.iter().any(|x| x == u) {
            continue;
        }
        ordered.push(u.clone());
    }
    ordered
}

pub async fn try_send_direct_then_proxy<N, D, K, C>(
    direct: &D,
    picker: &K,
    caller: &C,
    remote_write_urls: &[String],
    per_url_timeout: Duration,
    user_agent: &str,
    payload: Bytes,
    selected_url_opt: Option<String>,
) -> PromRemoteWriteAttemptResult<N>
where
    N: Clone + Send + Sync + std::fmt::Debug + 'static,
    D: PromRemoteWriteDirectSender,
    K: PromRemoteWriteProxyPicker<N>,
    C: PromRemoteWriteProxyCaller<N>,
{
    if remote_write_urls.is_empty() {
        return PromRemoteWriteAttemptResult::Disabled;
    }

    let ordered = order_urls(remote_write_urls, selected_url_opt.as_deref());
    for url in ordered.iter() {
        let rc = tokio::time::timeout(
            per_url_timeout,
            direct.send(url, user_agent, payload.clone()),
        )
        .await;
        match rc {
            Ok(Ok(())) => {
                return PromRemoteWriteAttemptResult::Sent {
                    path: PromRemoteWriteSendPath::Direct,
                    selected_url: Some(url.clone()),
                    proxy_node: None,
                };
            }
            Ok(Err(e)) => {
                debug!("remote-write direct attempt failed (url={}): {}", url, e);
            }
            Err(_) => {
                debug!(
                    "remote-write direct attempt timed out (url={}, timeout_s={})",
                    url,
                    per_url_timeout.as_secs()
                );
            }
        }
    }

    let Some(proxy_node) = picker.pick_proxy_node() else {
        return PromRemoteWriteAttemptResult::SkippedNoProxy {
            detail: format!(
                "remote-write failed locally; no proxy candidate available; candidates={:?}",
                remote_write_urls
            ),
        };
    };

    let req = PromRemoteWriteProxyReq {
        remote_write_urls: remote_write_urls.to_vec(),
        per_url_timeout_ms: per_url_timeout.as_millis() as u64,
        user_agent: user_agent.to_string(),
    };

    // Proxy handler may attempt every URL; set RPC timeout to cover that.
    let rpc_timeout = per_url_timeout
        .checked_mul(u32::try_from(remote_write_urls.len()).unwrap())
        .unwrap();

    let resp = match caller
        .call_proxy(proxy_node.clone(), req, payload, rpc_timeout)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(
                "remote-write proxy rpc failed (proxy_node={:?}): {}",
                proxy_node, e
            );
            return PromRemoteWriteAttemptResult::ProxyFailed {
                proxy_node,
                detail: format!("proxy rpc failed: {}", e),
            };
        }
    };

    if resp.ok {
        return PromRemoteWriteAttemptResult::Sent {
            path: PromRemoteWriteSendPath::Proxy,
            selected_url: resp.selected_url,
            proxy_node: Some(proxy_node),
        };
    }

    PromRemoteWriteAttemptResult::ProxyFailed {
        proxy_node,
        detail: resp.detail,
    }
}

pub async fn handle_proxy_request<D: PromRemoteWriteDirectSender>(
    direct: &D,
    req: PromRemoteWriteProxyReq,
    payload: Bytes,
) -> PromRemoteWriteProxyResp {
    let mut resp = PromRemoteWriteProxyResp::default();

    if req.remote_write_urls.is_empty() {
        resp.ok = false;
        resp.detail = "remote_write_urls is empty".to_string();
        return resp;
    }
    if req.user_agent.trim().is_empty() {
        resp.ok = false;
        resp.detail = "user_agent is empty".to_string();
        return resp;
    }
    if req.per_url_timeout_ms == 0 {
        resp.ok = false;
        resp.detail = "per_url_timeout_ms must be > 0".to_string();
        return resp;
    }

    let per_url_timeout = Duration::from_millis(req.per_url_timeout_ms);
    for url in req.remote_write_urls.iter() {
        let rc = tokio::time::timeout(
            per_url_timeout,
            direct.send(url, &req.user_agent, payload.clone()),
        )
        .await;
        match rc {
            Ok(Ok(())) => {
                resp.ok = true;
                resp.selected_url = Some(url.clone());
                resp.detail = "ok".to_string();
                return resp;
            }
            Ok(Err(e)) => {
                debug!("remote-write proxy attempt failed (url={}): {}", url, e);
            }
            Err(_) => {
                debug!(
                    "remote-write proxy attempt timed out (url={}, timeout_s={})",
                    url,
                    per_url_timeout.as_secs()
                );
            }
        }
    }

    resp.ok = false;
    resp.detail = "all remote_write_urls failed".to_string();
    resp
}
