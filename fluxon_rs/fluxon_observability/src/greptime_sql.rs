use anyhow::Context;
use reqwest::Url;
use serde::Deserialize;

#[derive(Clone, Debug)]
pub struct GreptimeSqlClient {
    base_url: String,
    db: String,
    http: reqwest::Client,
}

impl GreptimeSqlClient {
    pub fn new(base_url: String, db: String) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            db,
            http: reqwest::Client::new(),
        }
    }

    fn sql_url(&self) -> anyhow::Result<Url> {
        let s = format!("{}/v1/sql", self.base_url);
        let mut url = Url::parse(&s).with_context(|| {
            format!(
                "invalid greptime sql base_url (expected http(s)://..): {}",
                self.base_url
            )
        })?;
        url.query_pairs_mut().append_pair("db", &self.db);
        Ok(url)
    }

    pub async fn query_raw_json(&self, sql: &str) -> anyhow::Result<GreptimeSqlResponse> {
        let url = self.sql_url()?;
        let resp = self
            .http
            .post(url)
            .form(&[("sql", sql)])
            .send()
            .await
            .with_context(|| format!("greptime sql request failed: {}", sql))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .context("read greptime sql response body")?;
        if !status.is_success() {
            anyhow::bail!("greptime sql http {}: {}", status.as_u16(), body);
        }
        let parsed: GreptimeSqlResponse = serde_json::from_str(&body)
            .with_context(|| format!("parse greptime sql json: {}", body))?;
        Ok(parsed)
    }

    pub async fn describe_table(&self, table: &str) -> anyhow::Result<GreptimeDescribeTable> {
        let sql = format!("describe table {}", table);
        let resp = self.query_raw_json(&sql).await?;
        let out0 = resp
            .output
            .get(0)
            .context("missing output[0] in greptime sql response")?;
        let rec = out0
            .records
            .as_ref()
            .context("missing output[0].records in greptime sql response")?;

        // Describe table returns a fixed schema of 6 columns.
        // We keep this strict: if Greptime changes output format, fail loudly.
        let mut cols: Vec<DescribeColumn> = Vec::new();
        for row in &rec.rows {
            if row.len() != 6 {
                anyhow::bail!("unexpected describe table row len: {}", row.len());
            }
            let col_name = row[0]
                .as_str()
                .context("describe row[0] not string")?
                .to_string();
            let col_type = row[1]
                .as_str()
                .context("describe row[1] not string")?
                .to_string();
            let semantic_type = row[5]
                .as_str()
                .context("describe row[5] not string")?
                .to_string();
            cols.push(DescribeColumn {
                name: col_name,
                data_type: col_type,
                semantic_type,
            });
        }

        Ok(GreptimeDescribeTable { columns: cols })
    }
}

#[derive(Debug, Clone)]
pub struct GreptimeDescribeTable {
    pub columns: Vec<DescribeColumn>,
}

#[derive(Debug, Clone)]
pub struct DescribeColumn {
    pub name: String,
    pub data_type: String,
    pub semantic_type: String,
}

impl GreptimeDescribeTable {
    pub fn find_time_column(&self) -> anyhow::Result<&DescribeColumn> {
        // Greptime currently reports the time index column as semantic_type="TIMESTAMP".
        // Keep this as an explicit enum-like match to avoid string divergence.
        const TIME_SEMANTIC_TYPES: &[&str] = &["TIMESTAMP", "TIME INDEX", "TIME_INDEX"];
        for c in &self.columns {
            if TIME_SEMANTIC_TYPES.contains(&c.semantic_type.as_str()) {
                return Ok(c);
            }
        }
        anyhow::bail!("time index column not found in describe table")
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c.name == name)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GreptimeSqlResponse {
    pub output: Vec<GreptimeSqlOutput>,
    pub execution_time_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GreptimeSqlOutput {
    #[serde(default)]
    pub records: Option<GreptimeSqlRecords>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GreptimeSqlRecords {
    pub schema: GreptimeSqlSchema,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub total_rows: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GreptimeSqlSchema {
    pub column_schemas: Vec<GreptimeSqlColumnSchema>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GreptimeSqlColumnSchema {
    pub name: String,
    pub data_type: String,
}
