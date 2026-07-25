// src/main.rs

use std::error::Error;
use tracing::info;

use ninja::core::config::Config;
use ninja::server::orchestrator::Orchestrator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // tracing (ログ出力) の初期化
    tracing_subscriber::fmt::init();

    info!("=== 🥷 Ninja Distributed Master (Orchestrator) ===");

    // 設定の読み込み (Default トレイトの実装を使用)
    let config = Config::default();

    info!("📡 Worker 待機ポート: {}", config.worker_addr);
    info!("📡 Client 待機ポート: {}", config.client_addr);

    // オーケストレーターの初期化と実行
    let orchestrator = Orchestrator::from_config(config);
    orchestrator.run().await?;

    Ok(())
}