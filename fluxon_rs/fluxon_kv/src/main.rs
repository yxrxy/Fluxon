use limit_thirdparty::tokio;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fluxon_kv::entry().await
}
