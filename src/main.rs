mod core;

use std::sync::Arc;
use tokio::time::Duration;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::core::executor::{Executor, RemoteExecutor};
use crate::core::graph::{DagScheduler, Task};
use crate::core::packet::NinjaPacket;
use crate::core::worker::WorkerRegistry;
use crate::core::path::LeastLoadStrategy;
use crate::core::retry::DefaultRetryPolicy; // ✨ 新設したリトライポリシーをインポート

/// 【Data Plane】ネットワーク経由でパケットを受信して処理するリアルワーカー
async fn start_real_worker_server(address: String) {
    let listener = TcpListener::bind(&address).await.unwrap();
    let addr_clone = address.clone();
    
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut stream, _client_addr)) => {
                    let addr_sub = addr_clone.clone();
                    tokio::spawn(async move {
                        let mut len_buf = [0u8; 4];
                        if stream.read_exact(&mut len_buf).await.is_err() {
                            return;
                        }
                        let packet_len = u32::from_be_bytes(len_buf) as usize;

                        let mut packet_buf = vec![0u8; packet_len];
                        if stream.read_exact(&mut packet_buf).await.is_err() {
                            return;
                        }

                        if let Ok(packet) = NinjaPacket::from_bytes(&packet_buf) {
                            if let Ok(cmd_str) = String::from_utf8(packet.payload) {
                                println!("📥 [Worker: {}] パケット受信・解析完了 -> コマンド: '{}'", addr_sub, cmd_str);
                                
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                
                                let _ = stream.write_all(b"OK\n\n").await;
                            }
                        }
                    });
                }
                Err(_) => break,
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🥷 [Initialize] Ninja 分散タスクスケジューラを初期化中...");

    let worker1 = "127.0.0.1:8081".to_string();
    let worker2 = "127.0.0.1:8082".to_string();

    // リアルなネットワークを待ち受けるワーカーサーバーを非同期起動
    start_real_worker_server(worker1.clone()).await;
    println!("📡 [Data Plane/Worker] {} でTCPパケットレシーバーが稼働中...", worker1);
    
    start_real_worker_server(worker2.clone()).await;
    println!("📡 [Data Plane/Worker] {} でTCPパケットレシーバーが稼働中...", worker2);

    // WorkerRegistryの生成
    let registry = WorkerRegistry::new(vec![worker1, worker2]);

    // ハートビートのループ管理
    registry.start_heartbeat_loop(Duration::from_secs(2)).await;

    // 具体的なパス選択戦略オブジェクト（LeastLoadStrategy）を生成
    let strategy = Arc::new(LeastLoadStrategy);

    // ✨ 🥇 ① デフォルトのリトライポリシー（指数バックオフ内蔵）を生成
    let retry_policy = Arc::new(DefaultRetryPolicy::default());

    // 具象エグゼキュータにレジストリを渡して初期化
    let remote_executor = Arc::new(RemoteExecutor::new(registry.clone()));

    // スケジューラへ渡すために抽象トレイトの型（dyn Executor）へアップキャスト
    let executor: Arc<dyn Executor> = remote_executor;

    println!("✅ [Initialize] システムの初期化が正常に完了しました。");
    println!("🚀 [Main Loop] DAG構造 of 実行ループへ移行します。");

    // テスト用のDAGタスク構成
    let tasks = vec![
        Task {
            name: "task_A".to_string(),
            command: "echo 'Executing Task A'".to_string(),
            deps: vec![],
            timeout_secs: 5,
            max_retries: 3,
        },
        Task {
            name: "task_B".to_string(),
            command: "echo 'Executing Task B'".to_string(),
            deps: vec!["task_A".to_string()],
            timeout_secs: 5,
            max_retries: 3,
        },
        Task {
            name: "task_C".to_string(),
            command: "echo 'Executing Task C'".to_string(),
            deps: vec!["task_A".to_string()],
            timeout_secs: 5,
            max_retries: 1,
        },
        Task {
            name: "task_D".to_string(),
            command: "echo 'Executing Task D'".to_string(),
            deps: vec!["task_B".to_string(), "task_C".to_string()],
            timeout_secs: 5,
            max_retries: 5,
        },
    ];

    let mut scheduler = DagScheduler::new(tasks)?;
    
    // ✨ 修正: 抽象化された外出しオブジェクト達（executor, registry, strategy, retry_policy）を完全に引き渡す
    scheduler.run(executor, registry.clone(), strategy, retry_policy).await;

    println!("🧹 [Shutdown] システムの終了クリーンアップを開始します...");
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("🏁 [Shutdown] Ninja は安全に停止しました。");

    Ok(())
}