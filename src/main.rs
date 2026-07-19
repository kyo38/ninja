// src/main.rs

use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::AsyncReadExt;
use ninja::core::graph::{DagScheduler, LocalExecutor, Task};

/// 🔌 各クライアントからのTCP接続を処理する非同期関数
async fn handle_client(mut stream: TcpStream, executor: Arc<LocalExecutor>) {
    let mut buffer = vec![0; 65536]; // 64KBのバッファを確保

    println!("📥 [Ninja Server] クライアントからのデータを受信中...");

    match stream.read(&mut buffer).await {
        Ok(0) => {
            println!("⚠️ [Ninja Server] クライアントがデータを送らずに接続を閉じました。");
        }
        Ok(n) => {
            println!("📦 [Ninja Server] {} バイトのデータを受信。シリアライズ解析を開始します...", n);
            
            // バッファから有効な文字部分を抽出してJSON文字列に変換
            let json_str = match std::str::from_utf8(&buffer[..n]) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("❌ [Ninja Server] UTF-8 デコードエラー: {:?}", e);
                    return;
                }
            };

            // JSON から Vec<Task> へ復元
            match serde_json::from_str::<Vec<Task>>(json_str) {
                Ok(tasks) => {
                    println!("✓ [Ninja Server] タスクグラフのデシリアライズに成功しました (タスク数: {})", tasks.len());
                    
                    // スケジューラの初期化とトポロジカルソート（サイクルチェック）
                    match DagScheduler::new(tasks) {
                        Ok(scheduler) => {
                            println!("🚀 [Ninja Server] グラフ整合性チェック通過。アクターメインループを起動します。");
                            
                            // 構築したローカルアクターへ処理を移譲
                            scheduler.run(executor).await;
                        }
                        Err(e) => {
                            eprintln!("❌ [Ninja Server] スケジューラの初期化またはサイクル検出に失敗しました: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("❌ [Ninja Server] JSONのデシリアライズに失敗しました。構造体の定義が一致しているか確認してください: {:?}", e);
                    eprintln!("📄 受信データ内容: {}", json_str);
                }
            }
        }
        Err(e) => {
            eprintln!("❌ [Ninja Server] ストリーム読み込み中にエラーが発生しました: {:?}", e);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Ninja Distributed DAG Engine (Server Mode) ===");

    // 1. ローカルでのコマンド実行エンジン（Executor）の初期化
    let executor = Arc::new(LocalExecutor);
    
    // 2. 待ち受けポート（8080）のバインド
    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await?;
    println!("📡 [Ninja Server] サーバーが起動しました。接続を待機しています: {}", addr);

    // 3. サーバーメインループ（同時接続に備えて tokio::spawn で並行処理）
    loop {
        match listener.accept().await {
            Ok((stream, client_addr)) => {
                println!("🤝 [Ninja Server] 新しいクライアントが接続しました: {}", client_addr);
                
                let exec_clone = Arc::clone(&executor);
                
                // クライアントごとの処理をバックグラウンドに逃がし、サーバーは次の接続へ
                tokio::spawn(async move {
                    handle_client(stream, exec_clone).await;
                    println!("🔌 [Ninja Server] クライアント ( {} ) との通信処理が終了しました。\n", client_addr);
                });
            }
            Err(e) => {
                eprintln!("❌ [Ninja Server] 接続受け入れエラー: {:?}", e);
            }
        }
    }
}