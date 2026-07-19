use std::sync::Arc;
use tokio::time::Duration;
use tokio::net::TcpListener;
use tokio::io::AsyncReadExt;

mod core;
use crate::core::graph::{DagScheduler, RemoteExecutor, Task};

/// テスト用のダミーワーカーノードを起動する関数
async fn spawn_dummy_worker(addr: &'static str) {
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("📡 [Dummy Worker] {} でリクエスト待機中...", addr);

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut socket, _client_addr)) => {
                    let mut buf = [0; 1024];
                    tokio::spawn(async move {
                        while let Ok(n) = socket.read(&mut buf).await {
                            if n == 0 { break; }
                        }
                    });
                }
                Err(_) => break,
            }
        }
    });
}

#[tokio::main]
async fn main() {
    println!("🥷 [Ninja] 分散タスクスケジューラを起動しています...");

    // 1. ローカルホスト上にダミーのワーカーノードを2つバックグラウンドで起動
    spawn_dummy_worker("127.0.0.1:8081").await;
    spawn_dummy_worker("127.0.0.1:8082").await;

    // サーバーの起動を確実にするため少し待つ
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 2. ダミーのタスクセット（DAG）を構築
    let tasks = vec![
        Task {
            name: "task_A".to_string(),
            command: "echo 'Executing Task A'".to_string(),
            deps: vec![],
            timeout_secs: 30,
            max_retries: 3,
        },
        Task {
            name: "task_B".to_string(),
            command: "echo 'Executing Task B'".to_string(),
            deps: vec!["task_A".to_string()],
            timeout_secs: 30,
            max_retries: 3,
        },
        Task {
            name: "task_C".to_string(),
            command: "echo 'Executing Task C'".to_string(),
            deps: vec!["task_A".to_string()],
            timeout_secs: 60,
            max_retries: 1,
        },
        Task {
            name: "task_D".to_string(),
            command: "echo 'Executing Task D'".to_string(),
            deps: vec!["task_B".to_string(), "task_C".to_string()],
            timeout_secs: 10,
            max_retries: 5,
        },
    ];

    // 3. リモートエグゼキュータ（ワーカー管理）の初期化
    let worker_addresses = vec![
        "127.0.0.1:8081".to_string(),
        "127.0.0.1:8082".to_string(),
    ];
    let executor = Arc::new(RemoteExecutor::new(worker_addresses));

    // 4. ハートビーループの開始 (2秒間隔、タイムアウト500ms)
    executor.start_heartbeat_loop(Duration::from_secs(2), Duration::from_millis(500)).await;

    // 5. スケジューラの初期化と実行
    match DagScheduler::new(tasks) {
        Ok(mut scheduler) => {
            println!("✅ [Main] DAG構造の解析に成功しました。実行ループへ移行します。");
            scheduler.run(executor).await;
        }
        Err(err) => {
            eprintln!("❌ [Main] スケジューラの初期化に失敗しました: {}", err);
        }
    }

    // 完了後のハートビートを確認するため少し待って終了
    tokio::time::sleep(Duration::from_secs(2)).await;
}