// src/main.rs

use std::sync::Arc;
use std::fs;
use serde::Deserialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::AsyncReadExt;
use tokio::time::Duration;
use ninja::core::graph::{DagScheduler, RemoteExecutor, Task};

/// 📋 config.toml を読み込むための構造体定義
#[derive(Deserialize)]
struct Config {
    worker_addresses: Vec<String>,
    heartbeat: HeartbeatConfig,
}

#[derive(Deserialize)]
struct HeartbeatConfig {
    interval_secs: u64,
    timeout_secs: u64,
}

/// 🔌 各クライアントからのTCP接続を処理する非同期関数
async fn handle_client(mut stream: TcpStream, executor: Arc<RemoteExecutor>) {
    let mut buffer = vec![0; 65536];

    println!("📥 [Ninja Manager] クライアントからのデータを受信中...");

    match stream.read(&mut buffer).await {
        Ok(0) => {
            println!("⚠️ [Ninja Manager] クライアントがデータを送らずに接続を閉じました。");
        }
        Ok(n) => {
            println!("📦 [Ninja Manager] {} バイトのデータを受信。シリアライズ解析を開始します...", n);
            
            let json_str = match std::str::from_utf8(&buffer[..n]) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("❌ [Ninja Manager] UTF-8 デコードエラー: {:?}", e);
                    return;
                }
            };

            match serde_json::from_str::<Vec<Task>>(json_str) {
                Ok(tasks) => {
                    println!("✓ [Ninja Manager] タスクグラフのデシリアライズに成功しました (タスク数: {})", tasks.len());
                    
                    match DagScheduler::new(tasks) {
                        Ok(scheduler) => {
                            println!("🚀 [Ninja Manager] グラフ整合性チェック通過。アクターメインループを起動します。");
                            scheduler.run(executor).await;
                        }
                        Err(e) => {
                            eprintln!("❌ [Ninja Manager] スケジューラの初期化またはサイクル検出に失敗しました: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("❌ [Ninja Manager] JSONのデシリアライズに失敗しました: {:?}", e);
                    eprintln!("📄 受信データ内容: {}", json_str);
                }
            }
        }
        Err(e) => {
            eprintln!("❌ [Ninja Manager] ストリーム読み込み中にエラーが発生しました: {:?}", e);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Ninja Distributed DAG Engine (Manager Mode) ===");

    // 💡 1. config.toml から設定を動的にパース（ハードコーディングの排除）
    println!("📖 [Ninja Manager] 設定ファイル (config.toml) を読み込んでいます...");
    let config_content = fs::read_to_string("config.toml")
        .expect("❌ config.toml の読み込みに失敗しました。プロジェクトのルートに配置してください。");
    let config: Config = toml::from_str(&config_content)
        .expect("❌ config.toml の構文パースに失敗しました。");

    println!("✓ [Ninja Manager] 設定のロード完了。登録ワーカー数: {}", config.worker_addresses.len());

    // リモート接続対応の Executor を初期化
    let executor = Arc::new(RemoteExecutor::new(config.worker_addresses));
    
    // 💡 2. バックグラウンドでハートビート（死活監視）アクターを常時起動
    let hb_executor = Arc::clone(&executor);
    let interval = Duration::from_secs(config.heartbeat.interval_secs);
    let timeout_secs = Duration::from_secs(config.heartbeat.timeout_secs);
    
    tokio::spawn(async move {
        hb_executor.start_heartbeat_loop(interval, timeout_secs).await;
    });

    // クライアントからの要求を受け付ける待ち受けポート（8080）
    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await?;
    println!("📡 [Ninja Manager] マネージャーが起動しました。接続を待機しています: {}", addr);

    // サーバーメインループ
    loop {
        match listener.accept().await {
            Ok((stream, client_addr)) => {
                println!("🤝 [Ninja Manager] 新しいクライアントが接続しました: {}", client_addr);
                
                let exec_clone = Arc::clone(&executor);
                
                tokio::spawn(async move {
                    handle_client(stream, exec_clone).await;
                    println!("🔌 [Ninja Manager] クライアント ( {} ) との通信処理が終了しました。\n", client_addr);
                });
            }
            Err(e) => {
                eprintln!("❌ [Ninja Manager] 接続受け入れエラー: {:?}", e);
            }
        }
    }
}