use crate::greptime_otlp_log::LogCollectorAttrs;
use crate::greptime_otlp_log::build_export_body;
use crate::greptime_otlp_log_orchestrator::{
    GreptimeOtlpLogAttemptResult, GreptimeOtlpLogDirectSender, GreptimeOtlpLogProxyCaller,
    GreptimeOtlpLogProxyPicker, GreptimeOtlpLogProxyReq, GreptimeOtlpLogSendPath,
    try_send_direct_then_proxy,
};
use prost::bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::Subscriber;
use tracing::field::{Field, Visit};
use tracing::{debug, warn};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

const EXPORTER_TARGET: &str = "fluxon_observability::greptime_otlp_tracing";

#[derive(Clone)]
pub struct GreptimeOtlpTracingLayer {
    tx: tokio::sync::mpsc::Sender<String>,
    dropped: Arc<AtomicU64>,
}

pub struct GreptimeOtlpTracingReceiver {
    rx: tokio::sync::mpsc::Receiver<String>,
    dropped: Arc<AtomicU64>,
}

pub fn new_tracing_layer(
    max_queue_lines: usize,
) -> (GreptimeOtlpTracingLayer, GreptimeOtlpTracingReceiver) {
    let (tx, rx) = tokio::sync::mpsc::channel(max_queue_lines);
    let dropped = Arc::new(AtomicU64::new(0));
    (
        GreptimeOtlpTracingLayer {
            tx,
            dropped: dropped.clone(),
        },
        GreptimeOtlpTracingReceiver { rx, dropped },
    )
}

impl GreptimeOtlpTracingReceiver {
    pub fn into_inner(self) -> (tokio::sync::mpsc::Receiver<String>, Arc<AtomicU64>) {
        (self.rx, self.dropped)
    }
}

struct FieldCollector {
    message: Option<String>,
    kvs: Vec<(String, String)>,
}

impl FieldCollector {
    fn new() -> Self {
        Self {
            message: None,
            kvs: Vec::new(),
        }
    }

    fn push_kv(&mut self, field: &Field, v: String) {
        if field.name() == "message" {
            // "message" is the formatted string from tracing macros.
            self.message = Some(v);
            return;
        }
        self.kvs.push((field.name().to_string(), v));
    }
}

impl Visit for FieldCollector {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.push_kv(field, value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.push_kv(field, value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push_kv(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.push_kv(field, value.to_string());
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.push_kv(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.push_kv(field, format!("{:?}", value));
    }
}

impl<S> Layer<S> for GreptimeOtlpTracingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        if meta.target() == EXPORTER_TARGET {
            return;
        }
        let mut fields = FieldCollector::new();
        event.record(&mut fields);

        let mut line = String::new();

        // Keep format explicit and stable: Greptime will index by OTLP timestamp, not by parsing body.
        line.push_str(meta.level().as_str());
        line.push(' ');
        line.push_str(meta.target());

        if let (Some(file), Some(line_no)) = (meta.file(), meta.line()) {
            line.push_str(" (");
            line.push_str(file);
            line.push(':');
            line.push_str(&line_no.to_string());
            line.push(')');
        }

        line.push_str(": ");
        let has_message = fields.message.is_some();
        if let Some(msg) = fields.message.take() {
            line.push_str(&msg);
        }

        if !fields.kvs.is_empty() {
            for (i, (k, v)) in fields.kvs.iter().enumerate() {
                if has_message || i > 0 {
                    line.push(' ');
                }
                line.push_str(k);
                line.push('=');
                line.push_str(v);
            }
        }

        match self.tx.try_send(line) {
            Ok(()) => {}
            Err(_) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::debug;
    use tracing_subscriber::prelude::*;

    #[test]
    fn exporter_internal_logs_do_not_reenter_otlp_queue() {
        let (layer, rx) = new_tracing_layer(8);
        let subscriber = tracing_subscriber::registry().with(layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        debug!(target: EXPORTER_TARGET, "self log should be ignored");
        debug!(target: "fluxon_kv::demo", "user log should be captured");

        let (mut rx, _) = rx.into_inner();
        let captured = rx.try_recv().expect("expected one user log line");
        assert!(captured.contains("user log should be captured"));
        assert!(rx.try_recv().is_err());
    }
}

#[derive(Clone, Debug)]
pub struct GreptimeOtlpTracingExporterConfig {
    pub flush_interval: Duration,
    pub max_batch_lines: usize,
}

pub async fn run_exporter_loop<N, D, K, C, F>(
    exporter_cfg: GreptimeOtlpTracingExporterConfig,
    otlp_req: GreptimeOtlpLogProxyReq,
    attrs: LogCollectorAttrs,
    rx: GreptimeOtlpTracingReceiver,
    direct: D,
    picker: K,
    caller: C,
    shutdown: F,
) -> anyhow::Result<()>
where
    N: Clone + Send + Sync + std::fmt::Debug + 'static,
    D: GreptimeOtlpLogDirectSender,
    K: GreptimeOtlpLogProxyPicker<N>,
    C: GreptimeOtlpLogProxyCaller<N>,
    F: std::future::Future<Output = ()> + Send,
{
    let (mut rx, dropped) = rx.into_inner();

    let mut flush_tick = tokio::time::interval(exporter_cfg.flush_interval);
    let mut batch: Vec<String> = Vec::with_capacity(exporter_cfg.max_batch_lines.min(1024));

    let mut last_dropped: u64 = 0;
    let mut last_drop_report = tokio::time::Instant::now();

    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = flush_tick.tick() => {
                if !batch.is_empty() {
                    flush_one_batch::<N, _, _, _>(&direct, &picker, &caller, &otlp_req, &attrs, &mut batch).await;
                }

                let cur = dropped.load(Ordering::Relaxed);
                if cur != last_dropped && last_drop_report.elapsed() > Duration::from_secs(10) {
                    warn!(dropped = cur, "greptime otlp tracing exporter dropped log lines (queue full)");
                    last_dropped = cur;
                    last_drop_report = tokio::time::Instant::now();
                }
            }
            item = rx.recv() => {
                match item {
                    None => break,
                    Some(line) => {
                        batch.push(line);
                        if batch.len() >= exporter_cfg.max_batch_lines {
                            flush_one_batch::<N, _, _, _>(&direct, &picker, &caller, &otlp_req, &attrs, &mut batch).await;
                        }
                    }
                }
            }
            _ = &mut shutdown => {
                break;
            }
        }
    }

    if !batch.is_empty() {
        flush_one_batch::<N, _, _, _>(&direct, &picker, &caller, &otlp_req, &attrs, &mut batch)
            .await;
    }

    Ok(())
}

async fn flush_one_batch<N, D, K, C>(
    direct: &D,
    picker: &K,
    caller: &C,
    otlp_req: &GreptimeOtlpLogProxyReq,
    attrs: &LogCollectorAttrs,
    batch: &mut Vec<String>,
) where
    N: Clone + Send + Sync + std::fmt::Debug + 'static,
    D: GreptimeOtlpLogDirectSender,
    K: GreptimeOtlpLogProxyPicker<N>,
    C: GreptimeOtlpLogProxyCaller<N>,
{
    if batch.is_empty() {
        return;
    }

    let body = build_export_body(attrs, batch);
    let payload = Bytes::from(body);
    let res: GreptimeOtlpLogAttemptResult<N> =
        try_send_direct_then_proxy(direct, picker, caller, otlp_req, payload).await;

    match res {
        GreptimeOtlpLogAttemptResult::Disabled => {
            debug!("greptime otlp logs exporter disabled");
        }
        GreptimeOtlpLogAttemptResult::Sent { path, proxy_node } => match path {
            GreptimeOtlpLogSendPath::Direct => debug!("greptime otlp logs exported via direct"),
            GreptimeOtlpLogSendPath::Proxy => debug!(
                "greptime otlp logs exported via proxy (proxy_node={:?})",
                proxy_node
            ),
        },
        GreptimeOtlpLogAttemptResult::SkippedNoProxy { detail } => {
            debug!("greptime otlp logs export skipped (no proxy): {}", detail);
        }
        GreptimeOtlpLogAttemptResult::ProxyFailed { proxy_node, detail } => {
            debug!(
                "greptime otlp logs export proxy failed (proxy_node={:?}): {}",
                proxy_node, detail
            );
        }
    }

    batch.clear();
}
