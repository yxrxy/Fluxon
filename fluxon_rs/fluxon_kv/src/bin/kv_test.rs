// Feature-gated test binary wrapper
#![cfg(feature = "test_bins")]

use limit_thirdparty::tokio;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::args_os().len() > 1 {
        return fluxon_kv::entry().await;
    }

    fluxon_kv::kv_test::test_kv_all().await;
    Ok(())
}
