// src/bin/worker.rs

use std::error::Error;
use std::collections::HashMap;
use tokio::net::TcpListener;
use tokio::io::AsyncReadExt;
use ninja::core::graph::{Task, DagScheduler, TaskState};

/// 🛠️ OSプロセスを実行するコアロジック
async fn execute_command(name: &str, command: &str, timeout_secs: u64) -> bool {
    println!("⚡ [Worker] ➔ [{}] Running: {}", name, command);

    let mut cmd = if cfg!(target_os = "windows") {
        let mut c = tokio::process::Command::new("cmd");
        c.args(["/C", command]);
        c
    } else {
        let mut c = tokio::process::Command::new("sh");
        c.args(["-c", command]);
        c
    };

    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ [Worker] ➔ [{}] プロセスの起動に失敗: {:?}", name, e);
            return false;
        }
    };

    let result = if timeout_secs > 0 {
        tokio::time::timeout(tokio::time::Duration::from_secs(timeout_secs), child.wait()).await
    } else {
        Ok(child.wait().await)
    };

    match result {
        Ok(Ok(status)) => {
            if status.success() {
                println!("✓ [Worker] ➔ [{}] 正常終了", name);
                true
            } else {
                eprintln!("❌ [Worker] ➔ [{}] エラー終了 (Exit Code: {:?})", name, status.code());
                false
            }
        }
        Ok(Err(e)) => {
            eprintln!("❌ [Worker] ➔ [{}] プロセス待機中にエラー発生: {:?}", name, e);
            false
        }
        Err(_) => {
            eprintln!("⏱️ [Worker] ➔ [{}] タイムアウトしました ({} 秒制限)", name, timeout_secs);
            let _ = child.kill().await;
            false
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("=== 🥷 Ninja Test Server (Phase 1 Dynamic Runner) ===");

    // クライアント(client.rs)が接続してくるポート 8080 で待ち受ける
    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(&addr).await?;
    println!("📡 [Server] クライアントからのDAGタスク投入を待機中: {}", addr);

    loop {
        match listener.accept().await {
            Ok((mut stream, client_addr)) => {
                println!("🤝 [Server] クライアントが接続しました: {}", client_addr);

                // 大きめのバッファを確保してJSONを一括読み込み
                let mut buffer = vec![0; 65536];
                match stream.read(&mut buffer).await {
                    Ok(0) => continue,
                    Ok(n) => {
                        let json_str = std::str::from_utf8(&buffer[..n])?;
                        
                        // クライアントから送られてきた Vec<Task> をデシリアライズ
                        match serde_json::from_str::<Vec<Task>>(json_str) {
                            Ok(tasks) => {
                                println!("📦 正常に {} つのタスクを含むDAGを受信しました。スケジューラを開始します...", tasks.len());

                                // タスク名のルックアップ用マップと、タスクごとの実行状態管理マップを初期化
                                let mut task_lookup: HashMap<String, Task> = HashMap::new();
                                let mut state_map: HashMap<String, TaskState> = HashMap::new();

                                for task in tasks.iter() {
                                    task_lookup.insert(task.name.clone(), task.clone());
                                    state_map.insert(task.name.clone(), TaskState::Pending);
                                }

                                // フェーズ1で磨き上げた DagScheduler の初期化
                                let scheduler = match DagScheduler::new(tasks) {
                                    Ok(sched) => sched,
                                    Err(e) => {
                                        eprintln!("❌ [Server] DAGの初期化に失敗（循環参照など）: {:?}", e);
                                        continue;
                                    }
                                };

                                // スケジューラのループを実行
                                println!("\n--- 🚀 DAG 実行開始 ---");
                                loop {
                                    // 1. 今実行可能なタスクを抽出 (graph.rs の仕様に合わせて state_map を渡す)
                                    let ready_tasks = scheduler.get_ready_tasks(&state_map);
                                    
                                    // 全てのタスクが Success または Failed になっていれば終了判定
                                    let all_finished = state_map.values().all(|state| {
                                        matches!(state, TaskState::Success | TaskState::Failed)
                                    });

                                    if ready_tasks.is_empty() && all_finished {
                                        println!("🎉 全てのタスクの実行が完了しました！");
                                        break;
                                    }

                                    // もし準備完了タスクがなくて、全体も終わっていない（先行タスクの実行中など）場合
                                    if ready_tasks.is_empty() {
                                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                        continue;
                                    }

                                    // 2. 実行可能なタスクを順次処理
                                    for task_name in ready_tasks {
                                        if let Some(task) = task_lookup.get(&task_name) {
                                            // 状態を Running に遷移
                                            if let Some(state) = state_map.get_mut(&task_name) {
                                                *state = TaskState::Running;
                                            }
                                            
                                            // 実際にコマンドを実行
                                            let success = execute_command(&task.name, &task.command, task.timeout_secs).await;
                                            
                                            // 結果をローカルの状態マップにフィードバック
                                            if let Some(state) = state_map.get_mut(&task_name) {
                                                if success {
                                                    *state = TaskState::Success;
                                                } else {
                                                    *state = TaskState::Failed;
                                                }
                                            }
                                        }
                                    }
                                }
                                println!("--- 🏁 DAG 実行終了 ---\n");

                            }
                            Err(e) => {
                                eprintln!("❌ [Server] JSONのパースに失敗しました。構造が不一致の可能性があります: {:?}", e);
                            }
                        }
                    }
                    Err(e) => eprintln!("❌ [Server] 通信エラー: {:?}", e),
                }
            }
            Err(e) => eprintln!("❌ [Server] 接続受け入れエラー: {:?}", e),
        }
    }
}