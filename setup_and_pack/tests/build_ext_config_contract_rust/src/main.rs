use anyhow::Result;

fn main() -> Result<()> {
    // Read the three supported build_config_ext.yml fields via fluxon_util.
    let etcd = fluxon_util::dev_config::read_etcd_endpoint_from_build_config()?;
    let prom = fluxon_util::dev_config::load_tsdb_base_url()?;
    let prom_remote_write_url =
        fluxon_util::dev_config::read_prom_remote_write_url_from_build_config()?;

    let out = serde_json::json!({
        "etcd": etcd,
        "prom": prom,
        "prom_remote_write_url": prom_remote_write_url,
    });
    println!("{}", serde_json::to_string(&out)?);
    Ok(())
}
