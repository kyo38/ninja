// src/main.rs

use std::error::Error;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, Notify};
use ninja::core::graph::{Task, DagScheduler, TaskState};

/// 🟢 Workerの状態を管理する構造体
struct WorkerSession {
    id: usize,
    stream: TcpStream,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("==================================================");
    println!("🥷  Ninja Orchestrator Server (True Distributed Master) 🥷");
    println!("==================================================");

    // スケジュール状態と接続済みWorkerリストをスレッドセーフに管理
    let state_map: Arc<Mutex<HashMap<String, TaskState>>> = Arc::new(Mutex::new(HashMap::new()));
    let task_lookup: Arc<Mutex<HashMap<String, Task>>> = Arc::new(Mutex::new(HashMap::new()));
    let workers: Arc<Mutex<Vec<WorkerSession>>> = Arc::new(Mutex::new(Vec::new()));
    
    // タスク状態が更新されたり、Workerが追加されたときに検知するためのシグナル
    let pulse = Arc::new(Notify::new());

    // 1. Worker受付用サーバー (Port: 9001) をバックグラウンドで起動
    let worker_listener = TcpListener::bind("0.0.0.0:9001").await?;
    println!("📡 [Master] Workerからの接続を待機中... (ポート: 9001)");

    let workers_clone = Arc::clone(&workers);
    let pulse_clone = Arc::clone(&pulse);
    tokio::spawn(async move {
        let mut worker_id_counter = 0;
        loop {
            if let Ok((stream, addr)) = worker_listener.accept().await {
                worker_id_counter += 1;
                println!("🤝 [Master] Workerがクラスタに参加しました: {} (ID: {})", addr, worker_id_counter);
                
                let mut list = workers_clone.lock().await;
                list.push(WorkerSession { id: worker_id_counter, stream });
                
                // 新しいWorkerが来たことをスケジューラに通知
                pulse_clone.notify_waiters();
            }
        }
    });

    // 2. Client受付用サーバー (Port: 9000) を起動してDAGの投入を待つ
    let client_listener = TcpListener::bind("0.0.0.0:9000").await?;
    println!("📡 [Master] クライアントからのDAGタスク投入を待機中... (ポート: 9000)");

    loop {
        if let Ok((mut stream, addr)) = client_listener.accept().await {
            println!("📥 [Master] クライアントから接続されました: {}", addr);

            let mut buffer = vec![0; 65536];
            match stream.read(&mut buffer).await {
                Ok(0) => continue,
                Ok(n) => {
                    let json_str = std::str::from_utf8(&buffer[..n])?;
                    match serde_json::from_str::<Vec<Task>>(json_str) {
                        Ok(tasks) => {
                            println!("📦 正常に {} つのタスクを含むDAGを受信しました。中央集権スケジューラを開始します...", tasks.len());

                            // マップの初期化
                            {
                                let mut s_map = state_map.lock().await;
                                let mut t_map = task_lookup.lock().await;
                                s_map.clear();
                                t_map.clear();
                                for task in tasks.iter() {
                                    t_map.insert(task.name.clone(), task.clone());
                                    s_map.insert(task.name.clone(), TaskState::Pending);
                                }
                            }

                            // DAGスケジューラの初期化
                            let scheduler = match DagScheduler::new(tasks) {
                                Ok(sched) => sched,
                                Err(e) => {
                                    eprintln!("❌ [Master] DAGの初期化に失敗: {:?}", e);
                                    continue;
                                }
                            };

                            println!("\n--- 🚀 分散 DAG 実行スケジュール開始 ---");
                            
                            // メインの分散ディスパッチループ
                            loop {
                                let current_states = state_map.lock().await.clone();
                                
                                // すべてのタスクが正常終了または失敗したかチェック
                                let all_finished = current_states.values().all(|state| {
                                    matches!(state, TaskState::Success | TaskState::Failed)
                                });

                                if all_finished {
                                    println!("🎉 全ての分散タスクの実行が完了しました！");
                                    break;
                                }

                                // 💡 修正ポイント: 現在実行可能なタスクを取得
                                let ready_tasks = scheduler.get_ready_tasks(&current_states);

                                // 実行可能タスクがあり、かつ利用可能なWorkerがいるか確認
                                let mut worker_list = workers.lock().await;
                                if !ready_tasks.is_empty() && !worker_list.is_empty() {
                                    for task_name in ready_tasks {
                                        // 空いているWorkerを取り出す
                                        if let Some(mut worker) = worker_list.pop() {
                                            let task_lookup_map = task_lookup.lock().await;
                                            if let Some(task) = task_lookup_map.get(&task_name) {
                                                
                                                // 状態を Running に変更（これで次の周回でのフライングを防ぐ）
                                                if let Some(state) = state_map.lock().await.get_mut(&task_name) {
                                                    *state = TaskState::Running;
                                                }

                                                println!("✈️  [Master] Worker {} へタスク [{}] を配信します: {}", worker.id, task.name, task.command);
                                                
                                                let state_map_inner = Arc::clone(&state_map);
                                                let workers_inner = Arc::clone(&workers);
                                                let pulse_inner = Arc::clone(&pulse);
                                                let t_name = task.name.clone();
                                                let cmd_str = task.command.clone();

                                                // Workerとの通信・結果待ちを非同期スレッドで処理
                                                tokio::spawn(async move {
                                                    if worker.stream.write_all(cmd_str.as_bytes()).await.is_ok() {
                                                        let mut res_buf = vec![0; 1024];
                                                        if let Ok(bytes_read) = worker.stream.read(&mut res_buf).await {
                                                            let response = String::from_utf8_lossy(&res_buf[..bytes_read]);
                                                            let mut s_map = state_map_inner.lock().await;
                                                            if let Some(state) = s_map.get_mut(&t_name) {
                                                                if response.trim() == "SUCCESS" {
                                                                    println!("✓ [Master] Worker {} から報告: [{}] 正常終了", worker.id, t_name);
                                                                    *state = TaskState::Success;
                                                                } else {
                                                                    println!("❌ [Master] Worker {} から報告: [{}] エラー終了", worker.id, t_name);
                                                                    *state = TaskState::Failed;
                                                                }
                                                            }
                                                        }
                                                    } else {
                                                        println!("⚠️  [Master] Worker {} との通信に失敗。タスク [{}] を保留に戻します", worker.id, t_name);
                                                        if let Some(state) = state_map_inner.lock().await.get_mut(&t_name) {
                                                            *state = TaskState::Pending;
                                                        }
                                                    }
                                                    
                                                    // 役目を終えたWorkerをプールに戻す
                                                    workers_inner.lock().await.push(worker);
                                                    // 💡 状態が変わった（Successになった/Workerが戻った）ので、メインループを即座に起こす！
                                                    pulse_inner.notify_waiters();
                                                });
                                            } else {
                                                worker_list.push(worker);
                                            }
                                        } else {
                                            break; // 利用可能なWorkerが尽きた
                                        }
                                    }
                                }

                                // 💡 状態の変化（Notify）があるまで無駄なループをせず、美しくスリープして待つ
                                drop(worker_list); // ロックを外してから待機
                                pulse.notified().await;
                            }
                            println!("--- 🏁 分散 DAG 実行スケジュール終了 ---\n");
                        }
                        Err(e) => eprintln!("❌ [Master] JSONパースエラー: {:?}", e),
                    }
                }
                Err(e) => eprintln!("❌ [Master] クライアント通信エラー: {:?}", e),
            }
        }
    }
}