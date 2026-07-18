// src/main.rs
pub mod core {
    pub mod graph;
}
use ninja::platform::udp::UdpTransport;
use ninja::platform::abstraction::Transport;
use ninja::core::packet::{NinjaPacket, FLAG_ACK};
use core::graph::{Task, resolve_execution_order, Executor, LocalExecutor};
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::thread;

#[derive(Clone, Copy, Debug, PartialEq)]
enum TaskStatus {
    Pending,
    Running,
    Done,
    Failed,
}

fn main() {
    // コマンドライン引数の処理
    let args: Vec<String> = env::args().collect();
    
    let (my_as_id, listen_port) = if args.len() >= 3 {
        let as_id = args[1].parse::<u32>().expect("Invalid AS ID");
        let port = args[2].parse::<u16>().expect("Invalid Port");
        (as_id, port)
    } else {
        (100, 4000)
    };

    println!("=============================================");
    println!("  ninja router/server starting on AS {}...", my_as_id);
    println!("  Listening on 127.0.0.1:{}", listen_port);
    println!("=============================================");

    // 起動時セルフテスト
    println!("\n[Self-Test] 依存関係グラフ解析を実行中...");
    let test_tasks = vec![
        Task { name: "deploy".to_string(), deps: vec!["build".to_string(), "test".to_string()], command: "echo 'Deploying...'".to_string() },
        Task { name: "build".to_string(), deps: vec!["codegen".to_string()], command: "cargo build".to_string() },
        Task { name: "test".to_string(), deps: vec!["codegen".to_string()], command: "cargo test".to_string() },
        Task { name: "codegen".to_string(), deps: vec![], command: "echo 'Codegen...'".to_string() },
    ];
    if resolve_execution_order(&test_tasks).is_ok() {
        println!("[Self-Test] ✓ グラフの循環参照なし（DAG確認）。");
    }
    println!("---------------------------------------------\n");

    let mut topology = HashMap::new();
    topology.insert(2, "127.0.0.1:5000".to_string());

    let bind_addr = format!("127.0.0.1:{}", listen_port);
    let transport = UdpTransport::bind(&bind_addr);
    
    let executor = Arc::new(LocalExecutor);

    loop {
        let (data, addr) = transport.recv();

        match NinjaPacket::from_bytes(&data) {
            Ok(mut packet) => {
                let mut should_process_locally = true;

                if let Some(ref mut path_header) = packet.path {
                    if let Some(current_hop) = path_header.current_hop() {
                        if current_hop.as_id != my_as_id { continue; }
                        let egress_if = current_hop.egress_if;
                        let is_over = path_header.increment_hop();

                        if !is_over && egress_if != 0 {
                            should_process_locally = false;
                            if let Some(next_hop_str) = topology.get(&egress_if) {
                                if let Ok(next_hop_ip) = next_hop_str.parse::<SocketAddr>() {
                                    transport.send(&packet.to_bytes(), next_hop_ip);
                                }
                            }
                        }
                    }
                }

                if should_process_locally {
                    if packet.is_syn() {
                        if packet.payload.len() >= 2 {
                            let client_port = u16::from_be_bytes([packet.payload[0], packet.payload[1]]);
                            let real_client_addr = SocketAddr::new(addr.ip(), client_port);
                            let ack = NinjaPacket::new(FLAG_ACK, None, b"syn-ack".to_vec());
                            transport.send(&ack.to_bytes(), real_client_addr);
                        }
                    } else if packet.is_data() {
                        println!("🎉 [AS {}] Reached Final Destination! DATA packet received.", my_as_id);
                        
                        match serde_json::from_slice::<Vec<Task>>(&packet.payload) {
                            Ok(received_tasks) => {
                                println!("[AS {my_id}] ✓ タスク定義の受信に成功。DAGエンジンの駆動を開始します。\n", my_id = my_as_id);
                                
                                // =========================================================
                                // 🛠️ 1. イニシャルリセット（状態の初期化）
                                // =========================================================
                                let raw_states: HashMap<String, TaskStatus> = received_tasks
                                    .iter()
                                    .map(|t| (t.name.clone(), TaskStatus::Pending))
                                    .collect();
                                let task_states = Arc::new(Mutex::new(raw_states));
                                
                                // 完全イベント駆動用の完了通知チャネル (タスク名)
                                let (tx, rx) = mpsc::channel::<String>();

                                println!("🚀 [Ninja Engine] --- スケジューリングループ開始 ---");

                                // =========================================================
                                // 🔄 2. メインループ（完全イベント駆動型・制御された並列）
                                // =========================================================
                                let mut is_deadlocked = false;

                                loop {
                                    let mut ready_tasks = Vec::new();
                                    let mut has_running = false;
                                    let mut has_pending = false;

                                    // 【Scheduler】状態をスキャンして Pending ＆ 依存クリアなタスクを探す
                                    {
                                        let states = task_states.lock().unwrap();
                                        for task in &received_tasks {
                                            match states.get(&task.name) {
                                                Some(&TaskStatus::Pending) => {
                                                    has_pending = true;
                                                    let is_ready = task.deps.iter().all(|dep| {
                                                        states.get(dep) == Some(&TaskStatus::Done)
                                                    });
                                                    if is_ready {
                                                        ready_tasks.push(task.clone());
                                                    }
                                                }
                                                Some(&TaskStatus::Running) => {
                                                    has_running = true;
                                                }
                                                _ => {}
                                            }
                                        }
                                    }

                                    // 👉 【Scheduler ➔ Worker】発火処理
                                    if !ready_tasks.is_empty() {
                                        if ready_tasks.len() > 1 {
                                            let names: Vec<String> = ready_tasks.iter().map(|t| t.name.clone()).collect();
                                            println!("  [⚡ Parallel Ready] 同時並列実行を開始します: {:?}", names);
                                        }

                                        for task in ready_tasks {
                                            // 1手目：Scheduler側で即座に Running へロック固定（二重起動の完全防御）
                                            {
                                                let mut states = task_states.lock().unwrap();
                                                states.insert(task.name.clone(), TaskStatus::Running);
                                            }

                                            let tx_clone = tx.clone();
                                            let exec_clone = Arc::clone(&executor);
                                            let states_clone = Arc::clone(&task_states);

                                            // Workerスレッドの起動
                                            thread::spawn(move || {
                                                // タスク実行
                                                let success = exec_clone.execute(&task);
                                                
                                                // 2手目：Workerスレッド自身が責任を持って自分の状態を更新
                                                {
                                                    let mut states = states_clone.lock().unwrap();
                                                    if success {
                                                        states.insert(task.name.clone(), TaskStatus::Done);
                                                    } else {
                                                        states.insert(task.name.clone(), TaskStatus::Failed);
                                                    }
                                                }

                                                // 3手目：更新が終わったことをチャネルで通知 (notify)
                                                let _ = tx_clone.send(task.name);
                                            });
                                        }
                                        // 新しいスレッドを起こしたら、即座に次の状態を確認するためループの頭へ戻る
                                        continue;
                                    }

                                    // 👉 【Worker ➔ Scheduler】チャネルでの完了通知待ち
                                    // 実行中のもの（Running）があれば、通知が来るまでCPUを1ミリも使わずに完全ブロック待機
                                    if has_running {
                                        if let Ok(finished_task) = rx.recv() {
                                            // 誰かが終わったログ（※状態更新はWorker側で既に完了している）
                                            // これをトリガーに次の周回が回り、依存が外れた次のタスクがPendingからReadyになります
                                            let _ = finished_task; // 変数利用の明示
                                            continue;
                                        }
                                    }

                                    // デッドロック検知（動けないPendingがあるのに、走っているスレッドもない）
                                    if has_pending && !has_running {
                                        is_deadlocked = true;
                                        break;
                                    }

                                    // 実行中のタスクも未実行のタスクもなければ、安全に正常終了
                                    if !has_running {
                                        break;
                                    }
                                }

                                // =========================================================
                                // 🏁 3. 終了処理（結果判定とエラーハンドリング）
                                // =========================================================
                                let mut pending_tasks = Vec::new();
                                let mut failed_tasks = Vec::new();

                                {
                                    let states = task_states.lock().unwrap();
                                    for (name, status) in states.iter() {
                                        match status {
                                            TaskStatus::Pending | TaskStatus::Running => pending_tasks.push(name.clone()),
                                            TaskStatus::Failed => failed_tasks.push(name.clone()),
                                            TaskStatus::Done => {}
                                        }
                                    }
                                }

                                if is_deadlocked {
                                    println!("🛑 [Ninja Engine] 致命的エラー: デッドロック（循環依存または未定義の依存関係）を検出しました。");
                                    println!("  └── 実行不可能なタスク群: {:?}", pending_tasks);
                                    println!();
                                } else if !failed_tasks.is_empty() {
                                    println!("❌ [Ninja Engine] タスクの実行に失敗したため、後続処理を中断しました。失敗タスク: {:?}", failed_tasks);
                                } else {
                                    println!("🎉 [Ninja Engine] 全てのタスクグラフが依存関係通りに完全実行されました。\n");
                                }
                            }
                            Err(json_err) => {
                                eprintln!("[AS {}] ✗ パースエラー: {:?}", my_as_id, json_err);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                println!("[AS {}] Invalid packet: {:?}", my_as_id, e);
            }
        }
    }
}