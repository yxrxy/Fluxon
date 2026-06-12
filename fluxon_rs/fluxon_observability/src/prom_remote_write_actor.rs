use prost::bytes::Bytes;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::prom_remote_write_orchestrator::{
    PromRemoteWriteAttemptResult, PromRemoteWriteDirectSender, PromRemoteWriteProxyCaller,
    PromRemoteWriteProxyPicker, PromRemoteWriteSendPath, try_send_direct_then_proxy,
};
use fluxon_util::prom_remote_write::{TimeSeries, WriteRequest};

enum PromRemoteWriteActorMsg {
    UpdateRemoteWriteUrls {
        urls: Vec<String>,
    },
    SubmitCollected {
        metric_families: Vec<prometheus::proto::MetricFamily>,
        extra_timeseries: Vec<TimeSeries>,
    },
}

#[derive(Clone)]
pub struct PromRemoteWriteHandle {
    tx: mpsc::Sender<PromRemoteWriteActorMsg>,
}

impl PromRemoteWriteHandle {
    pub fn try_update_remote_write_urls(&self, urls: Vec<String>) {
        if let Err(e) = self
            .tx
            .try_send(PromRemoteWriteActorMsg::UpdateRemoteWriteUrls { urls })
        {
            warn!(
                "prom remote-write actor dropped UpdateRemoteWriteUrls: {}",
                e
            );
        }
    }

    pub fn try_submit_collected(
        &self,
        metric_families: Vec<prometheus::proto::MetricFamily>,
        extra_timeseries: Vec<TimeSeries>,
    ) {
        let metric_family_count = metric_families.len();
        let extra_timeseries_count = extra_timeseries.len();
        if let Err(e) = self.tx.try_send(PromRemoteWriteActorMsg::SubmitCollected {
            metric_families,
            extra_timeseries,
        }) {
            warn!(
                "prom remote-write actor dropped collected metrics: {} (metric_families={} extra_timeseries={})",
                e, metric_family_count, extra_timeseries_count
            );
        }
    }
}

pub struct PromRemoteWriteActorOwned<N, D, K, C> {
    rx: mpsc::Receiver<PromRemoteWriteActorMsg>,
    direct: D,
    picker: K,
    caller: C,
    per_url_timeout: Duration,
    user_agent: String,
    remote_write_urls: Vec<String>,
    selected_url: Option<String>,
    submit_count: u64,
    _phantom: std::marker::PhantomData<N>,
}

impl<N, D, K, C> PromRemoteWriteActorOwned<N, D, K, C>
where
    N: Clone + Send + Sync + std::fmt::Debug + 'static,
    D: PromRemoteWriteDirectSender,
    K: PromRemoteWriteProxyPicker<N>,
    C: PromRemoteWriteProxyCaller<N>,
{
    pub fn new(
        max_pending_payloads: usize,
        direct: D,
        picker: K,
        caller: C,
        per_url_timeout: Duration,
        user_agent: String,
    ) -> (PromRemoteWriteHandle, Self) {
        let (tx, rx) = mpsc::channel(max_pending_payloads);
        let handle = PromRemoteWriteHandle { tx };
        let owned = Self {
            rx,
            direct,
            picker,
            caller,
            per_url_timeout,
            user_agent,
            remote_write_urls: Vec::new(),
            selected_url: None,
            submit_count: 0,
            _phantom: std::marker::PhantomData,
        };
        (handle, owned)
    }

    pub async fn run<F>(mut self, shutdown: F)
    where
        F: std::future::Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);

        'actor: loop {
            tokio::select! {
                biased;

                _ = &mut shutdown => {
                    break;
                }
                maybe = self.rx.recv() => {
                    let Some(msg) = maybe else {
                        break;
                    };
                    match msg {
                        PromRemoteWriteActorMsg::UpdateRemoteWriteUrls { urls } => {
                            // If the URL set changes, clear selection to avoid pinning to a stale endpoint.
                            self.remote_write_urls = urls;
                            self.selected_url = None;
                        }
                        PromRemoteWriteActorMsg::SubmitCollected {
                            metric_families,
                            extra_timeseries,
                        } => {
                            let metric_family_count = metric_families.len();
                            let extra_timeseries_count = extra_timeseries.len();
                            let mut write_request = match WriteRequest::from_metric_families(metric_families, None) {
                                Ok(w) => w,
                                Err(e) => {
                                    warn!("prom remote-write failed to build WriteRequest: {}", e);
                                    continue;
                                }
                            };
                            if !extra_timeseries.is_empty() {
                                write_request.timeseries.extend(extra_timeseries);
                            }
                            let merged_timeseries_count = write_request.timeseries.len();
                            self.submit_count = self.submit_count.saturating_add(1);
                            let submit_seq = self.submit_count;
                            if extra_timeseries_count > 0 || should_log_debug_seq(submit_seq) {
                                debug!(
                                    "prom remote-write submit seq={} metric_families={} extra_timeseries={} merged_timeseries={} remote_write_urls={} selected_url={:?}",
                                    submit_seq,
                                    metric_family_count,
                                    extra_timeseries_count,
                                    merged_timeseries_count,
                                    self.remote_write_urls.len(),
                                    self.selected_url
                                );
                            }

                            let payload = match write_request.encode_compressed() {
                                Ok(b) => Bytes::from(b),
                                Err(e) => {
                                    warn!("prom remote-write failed to encode WriteRequest: {}", e);
                                    continue;
                                }
                            };
                            let rc: PromRemoteWriteAttemptResult<N> = tokio::select! {
                                biased;

                                _ = &mut shutdown => {
                                    break 'actor;
                                }
                                rc = try_send_direct_then_proxy(
                                    &self.direct,
                                    &self.picker,
                                    &self.caller,
                                    &self.remote_write_urls,
                                    self.per_url_timeout,
                                    &self.user_agent,
                                    payload,
                                    self.selected_url.clone(),
                                ) => rc,
                            };

                            match rc {
                                PromRemoteWriteAttemptResult::Disabled => {
                                    // Keep quiet: remote write can be intentionally disabled.
                                    debug!("prom remote-write disabled (remote_write_urls is empty)");
                                }
                                PromRemoteWriteAttemptResult::Sent {
                                    path,
                                    selected_url,
                                    proxy_node,
                                } => {
                                    self.selected_url = selected_url.clone();
                                    match path {
                                        PromRemoteWriteSendPath::Direct => {
                                            debug!(
                                                "prom remote-write sent directly (seq={} metric_families={} extra_timeseries={} merged_timeseries={} selected_url={:?})",
                                                submit_seq,
                                                metric_family_count,
                                                extra_timeseries_count,
                                                merged_timeseries_count,
                                                selected_url,
                                            );
                                        }
                                        PromRemoteWriteSendPath::Proxy => {
                                            debug!(
                                                "prom remote-write sent via proxy (seq={} metric_families={} extra_timeseries={} merged_timeseries={} proxy_node={:?}, selected_url={:?})",
                                                submit_seq,
                                                metric_family_count,
                                                extra_timeseries_count,
                                                merged_timeseries_count,
                                                proxy_node,
                                                selected_url
                                            );
                                        }
                                    }
                                }
                                PromRemoteWriteAttemptResult::SkippedNoProxy { detail } => {
                                    warn!("{}", detail);
                                }
                                PromRemoteWriteAttemptResult::ProxyFailed { proxy_node, detail } => {
                                    warn!(
                                        "prom remote-write proxy failed (proxy_node={:?}): {}",
                                        proxy_node, detail
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn should_log_debug_seq(seq: u64) -> bool {
    seq <= 8 || seq.is_power_of_two()
}
