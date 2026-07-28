use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    fake_server::cli::run(std::env::args().skip(1)).await
}
