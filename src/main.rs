// src/main.rs

use std::sync::Arc;
use ninja::core::graph::{DagScheduler, LocalExecutor, Task};

/// Tokio非同期ランタイムのエントリーポイント
#[tokio::main]
async fn main() {
    println!("=== Ninja DAG Engine (Async Mode) ===");

    // テスト用のサンプルDAGタスク群を構築
    // A, B -> C -> D の依存関係
    let tasks = vec![
        Task {
            name: "Task_A".to_string(),
            deps: vec![],
            command: "echo 'Executing A'".to_string(),
        },
        Task {
            name: "Task_B".to_string(),
            deps: vec![],
            command: "echo 'Executing B'".to_string(),
        },
        Task {
            name: "Task_C".to_string(),
            deps: vec!["Task_A".to_string(), "Task_B".to_string()],
            command: "echo 'Executing C after A and B'".to_string(),
        },
        Task {
            name: "Task_D".to_string(),
            deps: vec!["Task_C".to_string()],
            command: "echo 'Finalizing D'".to_string(),
        },
    ];

    println!("⚙️ タスクのバリデーションおよびグラフ初期化中...");
    
    match DagScheduler::new(tasks) {
        Ok(scheduler) => {
            println!("✓ グラフの整合性チェック通過 (サイクルなし)");
            
            let executor = Arc::new(LocalExecutor);
            let scheduler_arc = Arc::new(scheduler);

            // 非同期メインループを await で実行
            scheduler_arc.run(executor).await;
        }
        Err(e) => {
            eprintln!("❌ スケジューラの初期化に失敗しました: {:?}", e);
        }
    }
}