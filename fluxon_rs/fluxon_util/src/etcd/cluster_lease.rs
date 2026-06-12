use anyhow::{Context, Result, anyhow};
use etcd_client::{Client, Compare, CompareOp, GetOptions, PutOptions, Txn, TxnOp};
use tracing::debug;

/// Get or create a shared cluster lease id for a given logical key.
///
/// Port of Python `get_cluster_lease` in `fluxon_py.etcd`.
///
/// The lease id is stored in etcd under `cluster_lease/{lease_key}`.
/// All callers using the same `lease_key` will share the same lease id.
pub async fn get_cluster_lease_id(
    client: &mut Client,
    lease_key: &str,
    ttl_seconds: i64,
) -> Result<i64> {
    let key = format!("cluster_lease/{}", lease_key);

    // Fast path: read existing lease id
    let resp = client
        .get(key.clone(), None)
        .await
        .with_context(|| format!("failed to get cluster lease key {key}"))?;
    if let Some(kv) = resp.kvs().first() {
        let txt = String::from_utf8(kv.value().to_vec())
            .with_context(|| format!("invalid lease id bytes for key {key}"))?;
        let lease_id: i64 = txt
            .parse()
            .with_context(|| format!("invalid lease id '{}' for key {key}", txt))?;
        debug!(
            "reused existing cluster lease id {} for key {}",
            lease_id, key
        );
        return Ok(lease_id);
    }

    // Create a new lease and try to publish it atomically
    let lease_resp = client
        .lease_grant(ttl_seconds, None)
        .await
        .with_context(|| format!("failed to grant lease for key {}", key))?;
    let lease_id = lease_resp.id();

    let compare = Compare::create_revision(key.clone(), CompareOp::Equal, 0);
    let put_op = TxnOp::put(
        key.clone(),
        lease_id.to_string(),
        Some(PutOptions::new().with_lease(lease_id)),
    );
    let txn = Txn::new().when(vec![compare]).and_then(vec![put_op]);
    let txn_res = client
        .txn(txn)
        .await
        .with_context(|| format!("transaction failed when publishing cluster lease key {key}"))?;
    if txn_res.succeeded() {
        debug!(
            "published new cluster lease id {} for key {}",
            lease_id, key
        );
        return Ok(lease_id);
    }

    // Another creator won the race; read back
    let resp2 = client
        .get(key.clone(), Some(GetOptions::new()))
        .await
        .with_context(|| format!("failed to re-get cluster lease key {key}"))?;
    if let Some(kv) = resp2.kvs().first() {
        let txt = String::from_utf8(kv.value().to_vec())
            .with_context(|| format!("invalid lease id bytes for key {key} after txn"))?;
        let lease_id: i64 = txt
            .parse()
            .with_context(|| format!("invalid lease id '{}' for key {key} after txn", txt))?;
        debug!(
            "observed existing cluster lease id {} for key {} after txn",
            lease_id, key
        );
        return Ok(lease_id);
    }

    Err(anyhow!(
        "failed to acquire cluster lease for key {}: key disappeared after txn",
        lease_key
    ))
}
