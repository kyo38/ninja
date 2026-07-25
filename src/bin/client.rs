// src/bin/client.rs

use anyhow::Result;
use ninja::core::graph::Task;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<()> {
    println!("📡 [Client] Orchestrator へテスト用 DAG タスクを送信します...");

    // Master (Orchestrator) の Client 受信ポート (9090)
    let target_addr = "127.0.0.1:9090";
    let mut stream = TcpStream::connect(target_addr).await?;

    // リトライ・タイムアウト動作確認用の DAG シナリオ
    let tasks = vec![
        // Task A: 即座に成功する通常タスク
        Task {
            name: "Task_A".to_string(),
            command: "echo Task A completed successfully".to_string(),
            dependencies: vec![],
            timeout_secs: 5,
            max_retries: 0,
        },
        // Task B: 重い処理（スリープ）によるタイムアウトテスト
        // Worker側のデフォルトタイムアウト(10s)を超過させるため 15秒 スリープ
        Task {
            name: "Task_B_TimeoutTest".to_string(),
            command: "timeout /t 15 >nul".to_string(),
            dependencies: vec![],
            timeout_secs: 10,
            max_retries: 2,
        },
        // Task C: コマンドエラー（非ゼロ終了コード）による失敗リトライテスト
        Task {
            name: "Task_C_ErrorTest".to_string(),
            command: "cmd /c exit 1".to_string(),
            dependencies: vec!["Task_A".to_string()],
            timeout_secs: 5,
            max_retries: 3,
        },
        // Task D: 前提タスク（Task C）の依存関係確認用
        Task {
            name: "Task_D_Dependent".to_string(),
            command: "echo Task D completed".to_string(),
            dependencies: vec!["Task_C_ErrorTest".to_string()],
            timeout_secs: 5,
            max_retries: 0,
        },
    ];

    let json_payload = serde_json::to_string(&tasks)?;
    stream.write_all(json_payload.as_bytes()).await?;

    println!("✅ [Client] DAGタスクの送信が完了しました。");
    Ok(())
}