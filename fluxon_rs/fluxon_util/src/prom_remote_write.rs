use std::{collections::HashMap, time::SystemTime};

use prometheus::proto::MetricFamily;
use reqwest::Client;

/// Special Prometheus label for metric name
pub const LABEL_NAME: &str = "__name__";
pub const CONTENT_TYPE: &str = "application/x-protobuf";
pub const HEADER_NAME_REMOTE_WRITE_VERSION: &str = "X-Prometheus-Remote-Write-Version";
pub const REMOTE_WRITE_VERSION_01: &str = "0.1.0";
pub const COUNT_SUFFIX: &str = "_count";
pub const SUM_SUFFIX: &str = "_sum";
pub const TOTAL_SUFFIX: &str = "_total";
pub const BUCKET_SUFFIX: &str = "_bucket";
pub const BUCKET_COUNT_SUFFIX: &str = "_bucket_count";

#[derive(prost::Message, Clone, Hash, PartialEq, Eq)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(prost::Message, Clone, PartialEq)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

#[derive(prost::Message, Clone, PartialEq)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

impl TimeSeries {
    fn sort_labels_and_samples(&mut self) {
        self.labels.sort_by(|a, b| a.name.cmp(&b.name));
        self.samples.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    }
}

#[derive(prost::Message, Clone, PartialEq)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

impl WriteRequest {
    pub fn sort(&mut self) {
        for series in &mut self.timeseries {
            series.sort_labels_and_samples();
        }
    }

    fn sorted(mut self) -> Self {
        self.sort();
        self
    }

    pub fn encode_proto3(self) -> Vec<u8> {
        prost::Message::encode_to_vec(&self.sorted())
    }

    pub fn encode_compressed(self) -> Result<Vec<u8>, snap::Error> {
        snap::raw::Encoder::new().compress_vec(&self.encode_proto3())
    }

    pub fn from_metric_families(
        metric_families: Vec<MetricFamily>,
        custom_labels: Option<Vec<(String, String)>>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut timeseries = Vec::new();
        let now = current_timestamp_ms();
        let custom_labels = custom_labels.unwrap_or_default();

        for mf in metric_families.iter() {
            match mf.get_field_type() {
                prometheus::proto::MetricType::GAUGE => {
                    for m in mf.get_metric() {
                        let ts = if m.has_timestamp_ms() {
                            m.timestamp_ms()
                        } else {
                            now
                        };
                        let mut labels: Vec<(String, String)> = m
                            .get_label()
                            .iter()
                            .map(|l| (l.name().to_string(), l.value().to_string()))
                            .collect();
                        labels.push((LABEL_NAME.to_string(), mf.name().to_string()));
                        labels.extend(custom_labels.iter().cloned());

                        timeseries.push(TimeSeries {
                            labels: labels
                                .iter()
                                .map(|(k, v)| Label {
                                    name: k.to_string(),
                                    value: v.to_string(),
                                })
                                .collect(),
                            samples: vec![Sample {
                                value: m.get_gauge().value(),
                                timestamp: ts,
                            }],
                        });
                    }
                }
                prometheus::proto::MetricType::COUNTER => {
                    for m in mf.get_metric() {
                        let ts = if m.has_timestamp_ms() {
                            m.timestamp_ms()
                        } else {
                            now
                        };
                        let mut labels: Vec<(String, String)> = m
                            .get_label()
                            .iter()
                            .map(|l| (l.name().to_string(), l.value().to_string()))
                            .collect();
                        labels.push((LABEL_NAME.to_string(), mf.name().to_string()));
                        labels.extend(custom_labels.iter().cloned());

                        timeseries.push(TimeSeries {
                            labels: labels
                                .iter()
                                .map(|(k, v)| Label {
                                    name: k.to_string(),
                                    value: v.to_string(),
                                })
                                .collect(),
                            samples: vec![Sample {
                                value: m.get_counter().value(),
                                timestamp: ts,
                            }],
                        });
                    }
                }
                prometheus::proto::MetricType::SUMMARY => {
                    for m in mf.get_metric() {
                        let ts = if m.has_timestamp_ms() {
                            m.timestamp_ms()
                        } else {
                            now
                        };
                        let mut labels: HashMap<String, String> = m
                            .get_label()
                            .iter()
                            .map(|l| (l.name().to_string(), l.value().to_string()))
                            .collect();
                        labels.insert(LABEL_NAME.to_string(), mf.name().to_string());
                        for (k, v) in custom_labels.iter() {
                            labels.insert(k.clone(), v.clone());
                        }

                        for quantile in m.get_summary().get_quantile() {
                            let mut quantile_labels = labels.clone();
                            quantile_labels
                                .insert("quantile".to_string(), quantile.quantile().to_string());
                            timeseries.push(TimeSeries {
                                labels: quantile_labels
                                    .iter()
                                    .map(|(k, v)| Label {
                                        name: k.to_string(),
                                        value: v.to_string(),
                                    })
                                    .collect(),
                                samples: vec![Sample {
                                    value: quantile.value(),
                                    timestamp: ts,
                                }],
                            });
                        }

                        let mut top_labels = labels.clone();
                        top_labels.insert(
                            LABEL_NAME.to_string(),
                            format!("{}{}", mf.name(), SUM_SUFFIX),
                        );
                        timeseries.push(TimeSeries {
                            labels: top_labels
                                .iter()
                                .map(|(k, v)| Label {
                                    name: k.to_string(),
                                    value: v.to_string(),
                                })
                                .collect(),
                            samples: vec![Sample {
                                value: m.get_summary().sample_sum(),
                                timestamp: ts,
                            }],
                        });

                        let mut count_labels = labels.clone();
                        count_labels.insert(
                            LABEL_NAME.to_string(),
                            format!("{}{}", mf.name(), COUNT_SUFFIX),
                        );
                        timeseries.push(TimeSeries {
                            labels: count_labels
                                .iter()
                                .map(|(k, v)| Label {
                                    name: k.to_string(),
                                    value: v.to_string(),
                                })
                                .collect(),
                            samples: vec![Sample {
                                value: m.get_summary().sample_count() as f64,
                                timestamp: ts,
                            }],
                        });
                    }
                }
                prometheus::proto::MetricType::HISTOGRAM => {
                    for m in mf.get_metric() {
                        let ts = if m.has_timestamp_ms() {
                            m.timestamp_ms()
                        } else {
                            now
                        };
                        let mut base_labels: HashMap<String, String> = m
                            .get_label()
                            .iter()
                            .map(|l| (l.name().to_string(), l.value().to_string()))
                            .collect();
                        base_labels.insert(LABEL_NAME.to_string(), mf.name().to_string());
                        for (k, v) in custom_labels.iter() {
                            base_labels.insert(k.clone(), v.clone());
                        }

                        for bucket in m.get_histogram().get_bucket() {
                            let mut bucket_labels = base_labels.clone();
                            bucket_labels.insert(
                                LABEL_NAME.to_string(),
                                format!("{}{}", mf.name(), BUCKET_SUFFIX),
                            );
                            bucket_labels
                                .insert("le".to_string(), bucket.upper_bound().to_string());
                            timeseries.push(TimeSeries {
                                labels: bucket_labels
                                    .iter()
                                    .map(|(k, v)| Label {
                                        name: k.to_string(),
                                        value: v.to_string(),
                                    })
                                    .collect(),
                                samples: vec![Sample {
                                    value: bucket.cumulative_count() as f64,
                                    timestamp: ts,
                                }],
                            });
                        }

                        // +Inf bucket (total count)
                        let mut inf_labels = base_labels.clone();
                        inf_labels.insert(
                            LABEL_NAME.to_string(),
                            format!("{}{}", mf.name(), BUCKET_SUFFIX),
                        );
                        inf_labels.insert("le".to_string(), "+Inf".to_string());
                        timeseries.push(TimeSeries {
                            labels: inf_labels
                                .iter()
                                .map(|(k, v)| Label {
                                    name: k.to_string(),
                                    value: v.to_string(),
                                })
                                .collect(),
                            samples: vec![Sample {
                                value: m.get_histogram().get_sample_count() as f64,
                                timestamp: ts,
                            }],
                        });

                        let mut sum_labels = base_labels.clone();
                        sum_labels.insert(
                            LABEL_NAME.to_string(),
                            format!("{}{}", mf.name(), SUM_SUFFIX),
                        );
                        timeseries.push(TimeSeries {
                            labels: sum_labels
                                .iter()
                                .map(|(k, v)| Label {
                                    name: k.to_string(),
                                    value: v.to_string(),
                                })
                                .collect(),
                            samples: vec![Sample {
                                value: m.get_histogram().get_sample_sum(),
                                timestamp: ts,
                            }],
                        });

                        let mut count_labels = base_labels.clone();
                        count_labels.insert(
                            LABEL_NAME.to_string(),
                            format!("{}{}", mf.name(), COUNT_SUFFIX),
                        );
                        timeseries.push(TimeSeries {
                            labels: count_labels
                                .iter()
                                .map(|(k, v)| Label {
                                    name: k.to_string(),
                                    value: v.to_string(),
                                })
                                .collect(),
                            samples: vec![Sample {
                                value: m.get_histogram().get_sample_count() as f64,
                                timestamp: ts,
                            }],
                        });

                        let mut bucket_count_labels = base_labels;
                        bucket_count_labels.insert(
                            LABEL_NAME.to_string(),
                            format!("{}{}", mf.name(), BUCKET_COUNT_SUFFIX),
                        );
                        timeseries.push(TimeSeries {
                            labels: bucket_count_labels
                                .iter()
                                .map(|(k, v)| Label {
                                    name: k.to_string(),
                                    value: v.to_string(),
                                })
                                .collect(),
                            samples: vec![Sample {
                                value: m.get_histogram().get_sample_count() as f64,
                                timestamp: ts,
                            }],
                        });
                    }
                }
                prometheus::proto::MetricType::UNTYPED => {
                    // ignore
                }
            }
        }

        timeseries.sort_by(|a, b| {
            let name_a = a.labels.iter().find(|l| l.name == LABEL_NAME).unwrap();
            let name_b = b.labels.iter().find(|l| l.name == LABEL_NAME).unwrap();
            name_a.value.cmp(&name_b.value)
        });

        Ok(Self { timeseries }.sorted())
    }

    pub fn build_http_request(
        self,
        client: Client,
        endpoint: &str,
        user_agent: &str,
    ) -> Result<reqwest::Request, Box<dyn std::error::Error + Send + Sync>> {
        let compressed_body = self
            .encode_compressed()
            .map_err(|e| format!("Failed to compress metrics data: {}", e))?;

        let req = client
            .post(endpoint)
            .header(reqwest::header::CONTENT_TYPE, CONTENT_TYPE)
            .header(HEADER_NAME_REMOTE_WRITE_VERSION, REMOTE_WRITE_VERSION_01)
            .header(reqwest::header::CONTENT_ENCODING, "snappy")
            .header(reqwest::header::USER_AGENT, user_agent)
            .body(compressed_body)
            .build()?;

        Ok(req)
    }
}

fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
