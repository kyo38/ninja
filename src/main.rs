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
                                let (tx, rx) = mpsc::channel::<String>();

                                println!("🚀 [Ninja Engine] --- スケジューリングループ開始 ---");

                                // =========================================================
                                // 🔄 2. メインループ（「イベント」のみで駆動する設計）
                                // =========================================================
                                let mut is_deadlocked = false;

                                loop {
                                    let mut ready_tasks = Vec::new();
                                    let mut has_running = false;
                                    let mut has_pending = false;

                                    // 現在の状態から「次に叩けるタスク」を探索
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

                                    // 動かせるタスクがあれば、スレッドを一斉起動（投げっぱなしにせず、状態をRunningに固定）
                                    if !ready_tasks.is_empty() {
                                        if ready_tasks.len() > 1 {
                                            let names: Vec<String> = ready_tasks.iter().map(|t| t.name.clone()).collect();
                                            println!("  [⚡ Parallel Ready] 同時並列実行を開始します: {:?}", names);
                                        }

                                        for task in ready_tasks {
                                            {
                                                let mut states = task_states.lock().unwrap();
                                                states.insert(task.name.clone(), TaskStatus::Running);
                                            }

                                            let tx_clone = tx.clone();
                                            let exec_clone = Arc::clone(&executor);
                                            let states_clone = Arc::clone(&task_states);

                                            thread::spawn(move || {
                                                let success = exec_clone.execute(&task);
                                                {
                                                    let mut states = states_clone.lock().unwrap();
                                                    if success {
                                                        states.insert(task.name.clone(), TaskStatus::Done);
                                                    } else {
                                                        states.insert(task.name.clone(), TaskStatus::Failed);
                                                    }
                                                }
                                                // 【重要】タスクが完了したという「イベント」をメインに送信
                                                let _ = tx_clone.send(task.name);
                                            });
                                        }
                                        
                                        // ★修正ポイント: スレッドを投げたら、直後に continue で即時再スキャンするのをやめ、
                                        // そのまま下の「完了通知待ち（rx.recv）」へ流れ込ませます。
                                        // これにより、余分な空転ループが完全に排除されます。
                                        has_running = true; 
                                    }

                                    // デッドロックチェック：未実行タスクがあるのに、現在走っているスレッドもない場合
                                    if has_pending && !has_running {
                                        is_deadlocked = true;
                                        break;
                                    }

                                    // 全て完了：未実行もなく、走っているスレッドもないなら安全に終了
                                    if !has_pending && !has_running {
                                        break;
                                    }

                                    // 👉 【真のイベント駆動】バックグラウンドで走っている処理があるなら、
                                    // いずれかのWorkerから「完了通知（イベント）」が飛んでくるまで、ここで完全にスリープします。
                                    if has_running {
                                        if let Ok(finished_task) = rx.recv() {
                                            // 通知を受け取ったら、ループの先頭に戻って「そのイベントによって新しくReadyになったタスク」を1回だけスキャンします。
                                            let _ = finished_task;
                                        }
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