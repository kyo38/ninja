#![allow(dead_code)]

mod core;

use std::sync::Arc;
use crate::core::executor::{Executor, RemoteExecutor};
use crate::core::retry::DefaultRetryPolicy;
use crate::core::graph::Task;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Initializing Ninja Distributed Task Runner...");

    // 1. 各種コンポーネントの初期化
    let executor = Arc::new(RemoteExecutor::new());
    let retry_policy = Arc::new(DefaultRetryPolicy::default());

    // 2. サンプルタスクの構築（定義されているすべてのフィールドを満たすように修正）
    let sample_task = Task {
        name: "sample_compile_task".to_string(),
        command: "cargo build --release".to_string(),
        deps: vec![],                 // 依存タスク（空配列）
        max_retries: 3,               // 最大リトライ回数
        timeout_secs: 30,             // タイムアウト秒数
    };
    let worker_addr = "127.0.0.1:8080".to_string();

    println!("Submitting task: {}", sample_task.name);

    // 3. 実行とリトライロジックのハンドリング
    let mut current_retries = 0;
    let max_retries = sample_task.max_retries; // タスクに設定された値を使用

    loop {
        match executor.submit(sample_task.clone(), worker_addr.clone()).await {
            Ok(result) => {
                println!("Task execution status received: {:?}", result);
                
                // リトライポリシーによる判定
                use crate::core::retry::RetryPolicy;
                if retry_policy.should_retry(&result, current_retries, max_retries) {
                    current_retries += 1;
                    let wait_duration = retry_policy.backoff(current_retries);
                    println!(
                        "Retry condition met. Retrying in {}ms... (Attempt {}/{})",
                        wait_duration.as_millis(),
                        current_retries,
                        max_retries
                    );
                    tokio::time::sleep(wait_duration).await;
                    continue;
                }
                break;
            }
            Err(e) => {
                eprintln!("Fatal error communicating with executor: {}", e);
                break;
            }
        }
    }

    println!("Ninja process finished.");
    Ok(())
}