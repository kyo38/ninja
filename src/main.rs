// src/main.rs

use std::sync::Arc;
use ninja::core::graph::{DagScheduler, LocalExecutor, Task};

#[tokio::main]
async fn main() {
    println!("=== Ninja DAG Engine (Actor Mode) ===");

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

            // 💡 Arcでのラップを完全に撤廃し、所有権(self)をそのまま渡してアクターを駆動します
            scheduler.run(executor).await;
        }
        Err(e) => {
            eprintln!("❌ スケジューラの初期化に失敗しました: {:?}", e);
        }
    }
}