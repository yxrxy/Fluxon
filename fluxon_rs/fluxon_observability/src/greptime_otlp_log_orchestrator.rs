use anyhow::Context;
use async_trait::async_trait;
use bitcode::{Decode, Encode};
use prost::bytes::Bytes;
use std::time::Duration;
use tracing::{debug, warn};

use crate::keys::GREPTIME_LOG_EXTRACT_KEYS_HEADER_VALUE;

pub const GREPTIME_OTLP_LOG_PROXY_REQ_MSG_ID: u32 = 4301;
pub const GREPTIME_OTLP_LOG_PROXY_RESP_MSG_ID: u32 = 4302;

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GreptimeOtlpLogProxyReq {
    pub endpoint: String,
    pub db_name: String,
    pub table_name: Option<String>,
    /// Timeout applied for the HTTP POST attempt on the direct/proxy node.
    pub timeout_ms: u64,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GreptimeOtlpLogProxyResp {
    pub ok: bool,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GreptimeOtlpLogSendPath {
    Direct,
    Proxy,
}

#[derive(Clone, Debug)]
pub enum GreptimeOtlpLogAttemptResult<N> {
    Disabled,
    Sent {
        path: GreptimeOtlpLogSendPath,
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
pub trait GreptimeOtlpLogDirectSender: Send + Sync {
    async fn send(&self, req: &GreptimeOtlpLogProxyReq, payload: Bytes) -> anyhow::Result<()>;
}

#[derive(Clone, Debug)]
pub struct GreptimeOtlpLogHttpSender {
    http: reqwest::Client,
}

impl GreptimeOtlpLogHttpSender {
    pub fn new(http: reqwest::Client) -> Self {
        Self { http }
    }
}

#[async_trait]
impl GreptimeOtlpLogDirectSender for GreptimeOtlpLogHttpSender {
    async fn send(&self, req: &GreptimeOtlpLogProxyReq, payload: Bytes) -> anyhow::Result<()> {
        let mut reqb = self
            .http
            .post(&req.endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
            .header("X-Greptime-DB-Name", &req.db_name)
            .header(
                "X-Greptime-Log-Extract-Keys",
                GREPTIME_LOG_EXTRACT_KEYS_HEADER_VALUE,
            )
            .body(payload);

        if let Some(t) = req.table_name.as_ref() {
            reqb = reqb.header("X-Greptime-Log-Table-Name", t);
        }

        let resp = reqb.send().await.context("send otlp logs")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_else(|_| "".to_string());
            anyhow::bail!("greptime otlp http {}: {}", status.as_u16(), body);
        }
        Ok(())
    }
}

pub trait GreptimeOtlpLogProxyPicker<N>: Send + Sync {
    fn pick_proxy_node(&self) -> Option<N>;
}

#[async_trait]
pub trait GreptimeOtlpLogProxyCaller<N>: Send + Sync {
    async fn call_proxy(
        &self,
        proxy_node: N,
        req: GreptimeOtlpLogProxyReq,
        payload: Bytes,
        rpc_timeout: Duration,
    ) -> anyhow::Result<GreptimeOtlpLogProxyResp>;
}

pub async fn try_send_direct_then_proxy<N, D, K, C>(
    direct: &D,
    picker: &K,
    caller: &C,
    req: &GreptimeOtlpLogProxyReq,
    payload: Bytes,
) -> GreptimeOtlpLogAttemptResult<N>
where
    N: Clone + Send + Sync + std::fmt::Debug + 'static,
    D: GreptimeOtlpLogDirectSender,
    K: GreptimeOtlpLogProxyPicker<N>,
    C: GreptimeOtlpLogProxyCaller<N>,
{
    if req.endpoint.trim().is_empty() {
        return GreptimeOtlpLogAttemptResult::Disabled;
    }
    if req.timeout_ms == 0 {
        return GreptimeOtlpLogAttemptResult::Disabled;
    }

    let timeout = Duration::from_millis(req.timeout_ms);
    let direct_rc = tokio::time::timeout(timeout, direct.send(req, payload.clone())).await;
    let direct_fail_detail: Option<String> = match direct_rc {
        Ok(Ok(())) => {
            return GreptimeOtlpLogAttemptResult::Sent {
                path: GreptimeOtlpLogSendPath::Direct,
                proxy_node: None,
            };
        }
        Ok(Err(e)) => {
            // Keep a short, user-visible error for the "no proxy available" case.
            // Without this, the warning is misleading because proxy availability is not the root cause.
            debug!("otlp logs direct send failed: {}", e);
            Some(format!("{e:#}"))
        }
        Err(_) => {
            debug!(
                "otlp logs direct send timed out (timeout_s={})",
                timeout.as_secs()
            );
            Some(format!(
                "direct send timed out (timeout_s={})",
                timeout.as_secs()
            ))
        }
    };

    let Some(proxy_node) = picker.pick_proxy_node() else {
        let detail = direct_fail_detail.unwrap_or_else(|| "direct send failed".to_string());
        warn!(
            "otlp logs direct send failed; no proxy candidate available; endpoint={} detail={}",
            req.endpoint, detail
        );
        return GreptimeOtlpLogAttemptResult::SkippedNoProxy { detail };
    };

    let rpc_timeout = Duration::from_millis(req.timeout_ms.saturating_mul(2).max(1));
    let resp = match caller
        .call_proxy(proxy_node.clone(), req.clone(), payload, rpc_timeout)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(
                "otlp logs proxy rpc failed (proxy_node={:?}): {}",
                proxy_node, e
            );
            return GreptimeOtlpLogAttemptResult::ProxyFailed {
                proxy_node,
                detail: format!("proxy rpc failed: {}", e),
            };
        }
    };

    if resp.ok {
        return GreptimeOtlpLogAttemptResult::Sent {
            path: GreptimeOtlpLogSendPath::Proxy,
            proxy_node: Some(proxy_node),
        };
    }

    GreptimeOtlpLogAttemptResult::ProxyFailed {
        proxy_node,
        detail: resp.detail,
    }
}

pub async fn handle_proxy_request<D: GreptimeOtlpLogDirectSender>(
    direct: &D,
    req: GreptimeOtlpLogProxyReq,
    payload: Bytes,
) -> GreptimeOtlpLogProxyResp {
    let mut resp = GreptimeOtlpLogProxyResp::default();

    if req.endpoint.trim().is_empty() || !req.endpoint.contains("://") {
        resp.ok = false;
        resp.detail = format!("invalid endpoint: {}", req.endpoint);
        return resp;
    }
    if req.db_name.trim().is_empty() {
        resp.ok = false;
        resp.detail = "db_name is empty".to_string();
        return resp;
    }
    if req.timeout_ms == 0 {
        resp.ok = false;
        resp.detail = "timeout_ms must be > 0".to_string();
        return resp;
    }

    let timeout = Duration::from_millis(req.timeout_ms);
    let rc = tokio::time::timeout(timeout, direct.send(&req, payload)).await;
    match rc {
        Ok(Ok(())) => {
            resp.ok = true;
            resp.detail = "ok".to_string();
            resp
        }
        Ok(Err(e)) => {
            resp.ok = false;
            resp.detail = format!("direct send failed: {}", e);
            resp
        }
        Err(_) => {
            resp.ok = false;
            resp.detail = format!("direct send timed out (timeout_s={})", timeout.as_secs());
            resp
        }
    }
}
