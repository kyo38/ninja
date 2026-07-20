// src/bin/client.rs

use std::error::Error;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use ninja::core::graph::Task;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("📡 [Ninja Client] クラスタ/サーバーへの接続を試みています...");

    // 1. サーバーの待ち受けポート（デフォルト: 8080）に接続
    let addr = "127.0.0.1:9000";
    let mut stream = match TcpStream::connect(addr).await {
        Ok(s) => {
            println!("✓ [Ninja Client] サーバー ( {} ) に正常に接続しました。", addr);
            s
        }
        Err(e) => {
            eprintln!("❌ [Ninja Client] サーバーへの接続に失敗しました。サーバーが起動しているか確認してください: {:?}", e);
            return Err(e.into());
        }
    };

    // 2. サーバーへ送信するDAGタスクの構築
    // 💡 新しいフィールド timeout_secs と max_retries を追加して整合性を確保
    let test_tasks = vec![
        Task {
            name: "Task_A".to_string(),
            deps: vec![],
            command: "echo Executing A from Client".to_string(),
            timeout_secs: 5,  // クライアント送信時もデフォルトで5秒制限
            max_retries: 0,   // リトライなし
        },
        Task {
            name: "Task_B".to_string(),
            deps: vec![],
            command: "echo Executing B from Client".to_string(),
            timeout_secs: 5,
            max_retries: 0,
        },
        Task {
            name: "Task_C".to_string(),
            deps: vec!["Task_A".to_string()],
            command: "echo Executing C (Depends on A) from Client".to_string(),
            timeout_secs: 5,
            max_retries: 1,   // Cは万が一のために1回リトライ可能にする例
        },
        Task {
            name: "Task_D".to_string(),
            deps: vec!["Task_B".to_string(), "Task_C".to_string()],
            command: "echo Executing D (Final Leaf Task) from Client".to_string(),
            timeout_secs: 10, // 最終タスクは長めに確保
            max_retries: 0,
        },
    ];

    println!("📦 テスト用タスクグラフをJSONにシリアライズ中...");
    
    // 3. JSONへのシリアライズ
    let serialized_payload = match serde_json::to_string(&test_tasks) {
        Ok(json) => json,
        Err(e) => {
            eprintln!("❌ [Ninja Client] JSONシリアライズに失敗しました: {:?}", e);
            return Err(e.into());
        }
    };

    println!("📤 サーバーへタスクデータを送信しています (サイズ: {} bytes)...", serialized_payload.len());

    // 4. ストリームへペイロードを書き込み、接続を閉じる
    if let Err(e) = stream.write_all(serialized_payload.as_bytes()).await {
        eprintln!("❌ [Ninja Client] データ送信中にエラーが発生しました: {:?}", e);
        return Err(e.into());
    }

    // 確実にバッファをフラッシュ
    let _ = stream.flush().await;

    println!("🎉 [Ninja Client] タスクグラフの送信が正常に完了しました。接続をクローズします。");
    Ok(())
}