use std::error::Error;
use tracing_subscriber::{fmt, EnvFilter};

use ninja::core::config::Config;
use ninja::server::orchestrator::Orchestrator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // RUST_LOG環境変数によるレベル制御に対応した tracing の初期化
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(true)
        .init();

    // 設定の読み込み (Config のデフォルト値: worker_addr, client_addr)
    let config = Config::default();

    // オーケストレーターの初期化と実行
    let orchestrator = Orchestrator::from_config(config);
    orchestrator.run().await?;

    Ok(())
}