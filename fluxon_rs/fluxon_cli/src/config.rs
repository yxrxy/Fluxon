use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;

use anyhow::Context;

pub const AUTO_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
pub const DEFAULT_GREPTIME_SQL_DB: &str = "public";
pub const DEFAULT_GREPTIME_SQL_LOG_TABLE: &str = "fluxon_logs";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GreptimeSqlConfigYaml {
    pub base_url: String,
    pub db: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_table: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GreptimeSqlConfig {
    pub base_url: String,
    pub db: String,
    pub log_table: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormat {
    Cli,
    Web,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberKind {
    Kv,
    Mq,
    Fs,
}

pub const AVAILABLE_MEMBER_KINDS: &[MemberKind] = &[MemberKind::Kv, MemberKind::Mq, MemberKind::Fs];

impl MemberKind {
    pub fn parse_query_str(s: &str) -> Option<Self> {
        match s {
            "kv" => Some(MemberKind::Kv),
            "mq" => Some(MemberKind::Mq),
            "fs" => Some(MemberKind::Fs),
            _ => None,
        }
    }

    pub fn as_query_str(self) -> &'static str {
        match self {
            MemberKind::Kv => "kv",
            MemberKind::Mq => "mq",
            MemberKind::Fs => "fs",
        }
    }

    pub fn as_display_str(self) -> &'static str {
        match self {
            MemberKind::Kv => "kv",
            MemberKind::Mq => "mq",
            MemberKind::Fs => "fs",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorConfigYaml {
    pub etcd_endpoints: Vec<String>,
    /// Prometheus-compatible HTTP API base URL, e.g. `http://host:34030/v1/prometheus`.
    pub prometheus_base_url: String,
    pub cluster_name: String,
    pub member_kind: MemberKind,
    pub output: OutputFormat,
    /// Optional etcd key prefixes for discovering MQ unique_key -> chan_id mappings.
    ///
    /// Rationale: `new_or_bind_with_unique_key()` stores unique mappings under a
    /// dedicated etcd namespace, while the user-provided unique_id suffix remains arbitrary.
    /// Fluxon CLI must not scan the whole etcd keyspace by default.
    ///
    /// When provided, Fluxon CLI will scan these prefixes and interpret each matching key's
    /// value as a digit-only `chan_id` string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mq_unique_key_prefixes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_listen_addr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub greptime_sql: Option<GreptimeSqlConfigYaml>,
}

#[derive(Debug, Clone)]
pub struct MonitorConfig {
    pub etcd_endpoints: Vec<String>,
    pub prometheus_base_url: String,
    pub cluster_name: String,
    pub member_kind: MemberKind,
    pub output: OutputFormat,
    pub mq_unique_key_prefixes: Option<Vec<String>>,
    pub http_listen_addr: Option<String>,
    pub greptime_sql: Option<GreptimeSqlConfig>,
}

impl MonitorConfigYaml {
    pub fn from_file(path: &Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("read monitor config: {}", path.display()))?;
        let cfg: MonitorConfigYaml =
            serde_yaml::from_str(&s).with_context(|| "parse monitor config yaml".to_string())?;
        Ok(cfg)
    }

    pub fn verify(self) -> anyhow::Result<MonitorConfig> {
        if self.etcd_endpoints.is_empty() {
            anyhow::bail!("etcd_endpoints cannot be empty");
        }
        for e in &self.etcd_endpoints {
            if e.trim().is_empty() {
                anyhow::bail!("etcd_endpoints contains empty endpoint");
            }
            if !e.contains("://") {
                anyhow::bail!("etcd_endpoints must include protocol prefix: {}", e);
            }
        }

        if self.prometheus_base_url.trim().is_empty() {
            anyhow::bail!("prometheus_base_url cannot be empty");
        }
        if !self.prometheus_base_url.contains("://") {
            anyhow::bail!(
                "prometheus_base_url must include protocol prefix: {}",
                self.prometheus_base_url
            );
        }

        if self.cluster_name.trim().is_empty() {
            anyhow::bail!("cluster_name cannot be empty");
        }

        if let Some(prefixes) = self.mq_unique_key_prefixes.as_ref() {
            if prefixes.is_empty() {
                anyhow::bail!("mq_unique_key_prefixes cannot be empty when provided");
            }
            for p in prefixes {
                if p.trim().is_empty() {
                    anyhow::bail!("mq_unique_key_prefixes contains empty prefix");
                }
                if p != p.trim() {
                    anyhow::bail!(
                        "mq_unique_key_prefixes must not have leading/trailing whitespace: {p:?}"
                    );
                }
            }
        }

        let derived_greptime_sql =
            derive_greptime_sql_from_prometheus_base_url(self.prometheus_base_url.as_str());
        let greptime_sql = match self.greptime_sql.as_ref() {
            Some(cfg) => {
                let base = cfg.base_url.trim();
                if base.is_empty() || !base.contains("://") {
                    anyhow::bail!(
                        "greptime_sql.base_url must be http(s)://.., got: {}",
                        cfg.base_url
                    );
                }
                let db = cfg.db.trim();
                if db.is_empty() {
                    anyhow::bail!("greptime_sql.db cannot be empty");
                }

                // log_table is intentionally optional. We do not assume a default here because the
                // actual OTLP table name may vary by GreptimeDB config/pipeline.
                let log_table = match cfg.log_table.as_ref() {
                    Some(t) => {
                        let t = t.trim();
                        if t.is_empty() {
                            anyhow::bail!("greptime_sql.log_table cannot be empty when provided");
                        }
                        Some(t.to_string())
                    }
                    None => None,
                };

                Some(GreptimeSqlConfig {
                    base_url: base.trim_end_matches('/').to_string(),
                    db: db.to_string(),
                    log_table,
                })
            }
            None => derived_greptime_sql,
        };

        Ok(MonitorConfig {
            etcd_endpoints: self.etcd_endpoints,
            prometheus_base_url: self.prometheus_base_url.trim_end_matches('/').to_string(),
            cluster_name: self.cluster_name,
            member_kind: self.member_kind,
            output: self.output,
            mq_unique_key_prefixes: self.mq_unique_key_prefixes,
            http_listen_addr: self.http_listen_addr,
            greptime_sql,
        })
    }
}

fn derive_greptime_sql_from_prometheus_base_url(
    prometheus_base_url: &str,
) -> Option<GreptimeSqlConfig> {
    // Causal chain:
    // - Self-host and demo stacks already expose Greptime's Prometheus-compatible API at `/v1/prometheus`.
    // - The embedded logs views need Greptime SQL base_url/db/log_table, but duplicating that block in every
    //   monitor config is fragile and easy to miss during rollout.
    // - When the Prometheus endpoint clearly points at Greptime, derive the matching SQL config from the same
    //   origin so logs stay usable without introducing another divergent config source.
    let trimmed = prometheus_base_url.trim().trim_end_matches('/');
    let Some(base_url) = trimmed.strip_suffix("/v1/prometheus") else {
        return None;
    };
    if base_url.is_empty() {
        return None;
    }
    Some(GreptimeSqlConfig {
        base_url: base_url.to_string(),
        db: DEFAULT_GREPTIME_SQL_DB.to_string(),
        log_table: Some(DEFAULT_GREPTIME_SQL_LOG_TABLE.to_string()),
    })
}
