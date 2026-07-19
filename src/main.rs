// src/main.rs

use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::AsyncReadExt;
use ninja::core::graph::{DagScheduler, RemoteExecutor, Task};

/// 🔌 各クライアントからのTCP接続を処理する非同期関数
async fn handle_client(mut stream: TcpStream, executor: Arc<RemoteExecutor>) {
    let mut buffer = vec![0; 65536]; // 64KBのバッファを確保

    println!("📥 [Ninja Manager] クライアントからのデータを受信中...");

    match stream.read(&mut buffer).await {
        Ok(0) => {
            println!("⚠️ [Ninja Manager] クライアントがデータを送らずに接続を閉じました。");
        }
        Ok(n) => {
            println!("📦 [Ninja Manager] {} バイトのデータを受信。シリアライズ解析を開始します...", n);
            
            // バッファから有効な文字部分を抽出してJSON文字列に変換
            let json_str = match std::str::from_utf8(&buffer[..n]) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("❌ [Ninja Manager] UTF-8 デコードエラー: {:?}", e);
                    return;
                }
            };

            // JSON から Vec<Task> へ復元
            match serde_json::from_str::<Vec<Task>>(json_str) {
                Ok(tasks) => {
                    println!("✓ [Ninja Manager] タスクグラフのデシリアライズに成功しました (タスク数: {})", tasks.len());
                    
                    // スケジューラの初期化とトポロジカルソート（サイクルチェック）
                    match DagScheduler::new(tasks) {
                        Ok(scheduler) => {
                            println!("🚀 [Ninja Manager] グラフ整合性チェック通過。アクターメインループを起動します。");
                            
                            // 構築したリモートワーカープール対応アクターへ処理を移譲
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

    // 💡 SCIONの送信者主導思想に倣い、マネージャー側で利用可能なワーカーの「パス/ルート」を一元管理
    // 将来的に複数の独立した物理ノード（例: "192.168.1.50:9000", "192.168.1.51:9000"）を並べるだけでスケールします
    let worker_addresses = vec![
        "127.0.0.1:9000".to_string(),
    ];
    
    // リモート接続対応の Executor を初期化
    let executor = Arc::new(RemoteExecutor::new(worker_addresses));
    
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