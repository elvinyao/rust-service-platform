use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    service_runtime::cli::run().await
}
