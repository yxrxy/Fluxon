use crate::keys::{
    GREPTIME_LOG_EXTRACT_KEYS_HEADER_VALUE, KEY_CLUSTER_NAME, KEY_MEMBER_ID, KEY_MEMBER_KIND,
    KEY_ROLE,
};
use crate::types::{FluxonMemberKind, FluxonMemberRole};
use anyhow::Context;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::{debug, warn};

#[derive(Clone, Debug)]
pub struct GreptimeOtlpLogExporterConfig {
    pub endpoint: String,
    pub db_name: String,
    pub table_name: Option<String>,
}

#[derive(Clone, Debug)]
pub struct LogCollectorConfig {
    pub flush_interval_ms: u64,
    pub max_batch_lines: usize,
    pub max_queue_lines: usize,
}

#[derive(Clone, Debug)]
pub struct LogCollectorAttrs {
    pub cluster_name: String,
    pub member_kind: FluxonMemberKind,
    pub role: FluxonMemberRole,
    pub member_id: String,
}

pub async fn run_log_collector_from_file_tail_end<F>(
    exporter_cfg: GreptimeOtlpLogExporterConfig,
    collector_cfg: LogCollectorConfig,
    log_file_path: PathBuf,
    attrs: LogCollectorAttrs,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()> + Send,
{
    let exporter = GreptimeOtlpLogExporter::new(exporter_cfg)?;

    let mut file = tokio::fs::File::open(&log_file_path)
        .await
        .with_context(|| format!("open log file: {}", log_file_path.display()))?;
    file.seek(std::io::SeekFrom::End(0))
        .await
        .with_context(|| format!("seek log file end: {}", log_file_path.display()))?;

    let mut read_tick = tokio::time::interval(Duration::from_millis(100));
    let mut flush_tick =
        tokio::time::interval(Duration::from_millis(collector_cfg.flush_interval_ms));

    let mut buf: Vec<u8> = vec![0u8; 8192];
    let mut carry: Vec<u8> = Vec::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut dropped: u64 = 0;
    let mut last_drop_report = tokio::time::Instant::now();

    let mut batch: Vec<String> = Vec::with_capacity(collector_cfg.max_batch_lines.min(1024));

    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = read_tick.tick() => {
                match file.read(&mut buf).await {
                    Ok(0) => {}
                    Ok(n) => {
                        carry.extend_from_slice(&buf[..n]);
                        while let Some(pos) = carry.iter().position(|&b| b == b'\n') {
                            let mut line_bytes = carry.drain(..=pos).collect::<Vec<u8>>();
                            if let Some(b'\n') = line_bytes.last() {
                                line_bytes.pop();
                            }
                            if line_bytes.is_empty() {
                                continue;
                            }
                            let line = String::from_utf8_lossy(&line_bytes).to_string();
                            if queue.len() >= collector_cfg.max_queue_lines {
                                dropped += 1;
                                if dropped % (collector_cfg.max_queue_lines as u64) == 0
                                    || last_drop_report.elapsed() > Duration::from_secs(10)
                                {
                                    warn!(
                                        dropped,
                                        max_queue_lines = collector_cfg.max_queue_lines,
                                        "greptime log collector queue full; dropping lines"
                                    );
                                    last_drop_report = tokio::time::Instant::now();
                                }
                                continue;
                            }
                            queue.push_back(line);
                        }

                        if queue.len() >= collector_cfg.max_batch_lines {
                            export_one_batch(&exporter, &collector_cfg, &attrs, &mut queue, &mut batch).await;
                        }
                    }
                    Err(e) => {
                        warn!(path = %log_file_path.display(), err = %e, "read log file failed; collector exiting");
                        break;
                    }
                }
            }
            _ = flush_tick.tick() => {
                if !queue.is_empty() {
                    export_one_batch(&exporter, &collector_cfg, &attrs, &mut queue, &mut batch).await;
                }
            }
            _ = &mut shutdown => {
                break;
            }
        }
    }

    // Best-effort flush remaining queued lines on shutdown.
    while !queue.is_empty() {
        export_one_batch(&exporter, &collector_cfg, &attrs, &mut queue, &mut batch).await;
        if batch.is_empty() {
            break;
        }
    }

    Ok(())
}

async fn export_one_batch(
    exporter: &GreptimeOtlpLogExporter,
    cfg: &LogCollectorConfig,
    attrs: &LogCollectorAttrs,
    queue: &mut VecDeque<String>,
    batch: &mut Vec<String>,
) {
    batch.clear();
    while batch.len() < cfg.max_batch_lines {
        let Some(line) = queue.pop_front() else {
            break;
        };
        batch.push(line);
    }
    if batch.is_empty() {
        return;
    }

    if let Err(e) = exporter.export_lines(attrs, batch).await {
        warn!(err = %e, "greptime otlp log export failed; dropping batch");
    }
}

struct GreptimeOtlpLogExporter {
    endpoint: String,
    db_name: String,
    table_name: Option<String>,
    http: reqwest::Client,
}

impl GreptimeOtlpLogExporter {
    fn new(cfg: GreptimeOtlpLogExporterConfig) -> anyhow::Result<Self> {
        let endpoint = cfg.endpoint.trim().to_string();
        if endpoint.is_empty() || !endpoint.contains("://") {
            anyhow::bail!(
                "invalid greptime otlp endpoint (expected http(s)://..): {}",
                endpoint
            );
        }
        if cfg.db_name.trim().is_empty() {
            anyhow::bail!("greptime db_name cannot be empty");
        }
        Ok(Self {
            endpoint,
            db_name: cfg.db_name.trim().to_string(),
            table_name: cfg
                .table_name
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            http: reqwest::Client::new(),
        })
    }

    async fn export_lines(
        &self,
        attrs: &LogCollectorAttrs,
        lines: &[String],
    ) -> anyhow::Result<()> {
        if lines.is_empty() {
            return Ok(());
        }

        let req = build_export_request(attrs, lines);
        let body = req.encode_to_vec();

        let mut reqb = self
            .http
            .post(&self.endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
            .header("X-Greptime-DB-Name", &self.db_name)
            .header(
                "X-Greptime-Log-Extract-Keys",
                GREPTIME_LOG_EXTRACT_KEYS_HEADER_VALUE,
            )
            .body(body);

        // Table name is intentionally optional:
        // - When omitted, GreptimeDB stores logs into its default table (currently "opentelemetry_logs").
        // - This keeps configuration minimal and avoids guessing a project-specific table name.
        if let Some(t) = self.table_name.as_ref() {
            reqb = reqb.header("X-Greptime-Log-Table-Name", t);
        }

        let resp = reqb.send().await.context("send otlp logs")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_else(|_| "".to_string());
            anyhow::bail!("greptime otlp http {}: {}", status.as_u16(), body);
        }
        debug!("greptime otlp logs exported: {} lines", lines.len());
        Ok(())
    }
}

pub fn build_export_body(attrs: &LogCollectorAttrs, lines: &[String]) -> Vec<u8> {
    let req = build_export_request(attrs, lines);
    req.encode_to_vec()
}

fn build_export_request(attrs: &LogCollectorAttrs, lines: &[String]) -> ExportLogsServiceRequest {
    let mut log_records: Vec<LogRecord> = Vec::with_capacity(lines.len());
    for l in lines {
        let now_ns = now_unix_nano();
        let (severity_text, severity_number) = parse_severity_from_prefixed_line(l);
        let kvs = vec![
            KeyValue {
                key: KEY_CLUSTER_NAME.to_string(),
                value: Some(any_string_value(attrs.cluster_name.clone())),
            },
            KeyValue {
                key: KEY_MEMBER_KIND.to_string(),
                value: Some(any_string_value(attrs.member_kind.as_str().to_string())),
            },
            KeyValue {
                key: KEY_ROLE.to_string(),
                value: Some(any_string_value(attrs.role.as_str().to_string())),
            },
            KeyValue {
                key: KEY_MEMBER_ID.to_string(),
                value: Some(any_string_value(attrs.member_id.clone())),
            },
        ];

        log_records.push(LogRecord {
            time_unix_nano: now_ns,
            observed_time_unix_nano: 0,
            trace_id: Vec::new(),
            span_id: Vec::new(),
            flags: 0,
            severity_number,
            severity_text,
            body: Some(any_string_value(l.clone())),
            attributes: kvs,
            dropped_attributes_count: 0,
        });
    }

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(any_string_value("fluxon".to_string())),
                }],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "fluxon".to_string(),
                    version: "".to_string(),
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                }),
                log_records,
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }],
    }
}

fn any_string_value(v: String) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(v)),
    }
}

fn parse_severity_from_prefixed_line(line: &str) -> (String, i32) {
    // The tracing layer prefixes each line with an uppercase level token ("INFO", "WARN", ...).
    // We derive OTLP severity fields from that prefix so:
    // - GreptimeDB can populate severity columns
    // - the web /logs view can filter quickly by severity
    //
    // OTLP severity_number mapping uses the base values for each level range:
    // TRACE=1, DEBUG=5, INFO=9, WARN=13, ERROR=17.
    let Some((lvl, _rest)) = line.split_once(' ') else {
        return ("".to_string(), 0);
    };
    match lvl {
        "TRACE" => ("TRACE".to_string(), 1),
        "DEBUG" => ("DEBUG".to_string(), 5),
        "INFO" => ("INFO".to_string(), 9),
        "WARN" => ("WARN".to_string(), 13),
        "ERROR" => ("ERROR".to_string(), 17),
        _ => ("".to_string(), 0),
    }
}

fn now_unix_nano() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
