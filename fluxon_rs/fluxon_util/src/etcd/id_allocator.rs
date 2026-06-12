use anyhow::{Context, Result, anyhow};
use etcd_client::{Client, Compare, CompareOp, PutOptions, Txn, TxnOp};
use tracing::debug;

/// Distributed ID allocator backed by etcd.
///
/// Port of Python `DistributeIdAllocator` in `fluxon_py.etcd`.
///
/// Global counter key: `dist_id_allocator/{prefix}`.
///
/// The counter key may either:
/// - reuse a caller-provided lease for channel-scoped allocators, or
/// - stay intentionally unleased for process-global monotonic counters.
pub struct DistributeIdAllocator {
    client: Client,
    prefix: String,
    lease_id: Option<i64>,
}

impl DistributeIdAllocator {
    /// Create a new allocator with the given etcd client, prefix and
    /// associated lease id.
    pub fn new(client: Client, prefix: impl Into<String>, lease_id: i64) -> Self {
        Self {
            client,
            prefix: prefix.into(),
            lease_id: Some(lease_id),
        }
    }

    /// Create a new allocator whose counter key is intentionally not bound to
    /// any etcd lease.
    ///
    /// This mode is required for process-global monotonic counters such as the
    /// top-level MPSC `chan_id` allocator. Binding that counter to a short-lived
    /// lease would let the key disappear during an idle window and later restart
    /// from `1`, which can collide with still-existing historical metadata.
    pub fn new_without_lease(client: Client, prefix: impl Into<String>) -> Self {
        Self {
            client,
            prefix: prefix.into(),
            lease_id: None,
        }
    }

    /// Allocate the next ID (starting from 1) using etcd transactions.
    ///
    /// This mirrors the Python logic:
    /// - Read current value once.
    /// - Try up to 100 times with compare-and-set on the value.
    pub async fn allocate_id(&self) -> Result<i64> {
        let key = format!("dist_id_allocator/{}", self.prefix);
        let mut client = self.client.clone();

        // Initial read of the current value (if any)
        let resp = client
            .get(key.clone(), None)
            .await
            .with_context(|| format!("failed to get dist_id key {key}"))?;
        let mut old_value_v: Option<Vec<u8>> = resp.kvs().first().map(|kv| kv.value().to_vec());

        for _ in 0..100 {
            // Parse current value as integer; default to 0 on any error
            let mut old_value_int: i64 = 0;
            if let Some(ref v) = old_value_v {
                if let Ok(s) = std::str::from_utf8(v) {
                    if let Ok(parsed) = s.parse::<i64>() {
                        old_value_int = parsed;
                    }
                }
            }

            if old_value_v.is_none() {
                // First-time create: only succeed if key does not exist
                let compare = Compare::create_revision(key.clone(), CompareOp::Equal, 0);
                let put_op = match self.lease_id {
                    Some(lease_id) => {
                        let put_opts = PutOptions::new().with_lease(lease_id);
                        TxnOp::put(key.clone(), "1", Some(put_opts))
                    }
                    None => TxnOp::put(key.clone(), "1", None),
                };
                let txn = Txn::new().when(vec![compare]).and_then(vec![put_op]);
                let txn_res = client.txn(txn).await.with_context(|| {
                    format!("transaction failed when creating dist_id key {key}")
                })?;
                if txn_res.succeeded() {
                    debug!("created dist_id key {} with value 1", key);
                    return Ok(1);
                }
            } else {
                // Compare-and-set on existing value
                let expected = old_value_v.clone().unwrap_or_default();
                let compare = Compare::value(key.clone(), CompareOp::Equal, expected);
                let new_int = old_value_int + 1;
                let put_op = match self.lease_id {
                    Some(lease_id) => {
                        let put_opts = PutOptions::new().with_lease(lease_id);
                        TxnOp::put(key.clone(), new_int.to_string(), Some(put_opts))
                    }
                    None => TxnOp::put(key.clone(), new_int.to_string(), None),
                };
                let txn = Txn::new().when(vec![compare]).and_then(vec![put_op]);
                let txn_res = client.txn(txn).await.with_context(|| {
                    format!("transaction failed when updating dist_id key {key}")
                })?;
                if txn_res.succeeded() {
                    debug!("updated dist_id key {} to value {}", key, new_int);
                    return Ok(new_int);
                }
            }

            // On failure, advance our local guess and try again, just like Python.
            let next_int = old_value_int + 1;
            old_value_v = Some(next_int.to_string().into_bytes());
        }

        Err(anyhow!(
            "DistributeIdAllocator with prefix {} failed to allocate id after 100 retries",
            self.prefix
        ))
    }
}
