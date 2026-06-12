#![cfg(feature = "test_bins")]
use limit_thirdparty::tokio;

#[tokio::main]
async fn main() {
    fluxon_kv::external_client_api::external_client_test::test_external_client_lifetime().await;
    // eprintln!("external_client_test binary: enable specific scenarios as needed.");
}
