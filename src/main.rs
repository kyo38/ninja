// src/main.rs
pub mod core {
    pub mod graph;
}
use ninja::platform::udp::UdpTransport;
use ninja::platform::abstraction::Transport;
use ninja::core::packet::{NinjaPacket, FLAG_ACK};
// 【プロ仕様化 ④】Executor と LocalExecutor をインポート
use core::graph::{Task, resolve_execution_order, Executor, LocalExecutor};
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;

#[derive(Clone, Copy, Debug, PartialEq)]
enum TaskStatus {
    Pending,
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

    // 【プロ仕様化 ④】使用するエグゼキュータをここでインスタンス化
    let executor = LocalExecutor;

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
                                println!("[AS {}] ✓ タスク定義の受信に成功。DAGエンジンの駆動を開始します。\n", my_as_id);
                                
                                let mut task_states: HashMap<String, TaskStatus> = received_tasks
                                    .iter()
                                    .map(|t| (t.name.clone(), TaskStatus::Pending))
                                    .collect();

                                println!("🚀 [Ninja Engine] --- スケジューリングループ開始 ---");

                                loop {
                                    let mut progress = false;
                                    let mut ready_tasks = Vec::new();

                                    for task in &received_tasks {
                                        if task_states.get(&task.name) != Some(&TaskStatus::Pending) {
                                            continue;
                                        }

                                        let is_ready = task.deps.iter().all(|dep| {
                                            task_states.get(dep) == Some(&TaskStatus::Done)
                                        });

                                        if is_ready {
                                            ready_tasks.push(task.clone());
                                        }
                                    }

                                    if !ready_tasks.is_empty() {
                                        if ready_tasks.len() > 1 {
                                            let names: Vec<String> = ready_tasks.iter().map(|t| t.name.clone()).collect();
                                            println!("  [⚡ Parallel Ready] 同時実行可能なタスク群を検知: {:?}", names);
                                        }

                                        for task in ready_tasks {
                                            // 【プロ仕様化 ④】ハードコードされていた実行部を分離したExecutorへ委譲
                                            let success = executor.execute(&task);

                                            if success {
                                                task_states.insert(task.name.clone(), TaskStatus::Done);
                                            } else {
                                                task_states.insert(task.name.clone(), TaskStatus::Failed);
                                                println!("  ❌ [Execute] ➔ [{}] Failed", task.name);
                                            }
                                            progress = true;
                                        }
                                    }

                                    if !progress {
                                        break;
                                    }
                                }

                                let mut pending_tasks = Vec::new();
                                let mut failed_tasks = Vec::new();

                                for (name, status) in &task_states {
                                    match status {
                                        TaskStatus::Pending => pending_tasks.push(name.clone()),
                                        TaskStatus::Failed => failed_tasks.push(name.clone()),
                                        TaskStatus::Done => {}
                                    }
                                }

                                if pending_tasks.is_empty() && failed_tasks.is_empty() {
                                    println!("🎉 [Ninja Engine] 全てのタスクグラフが依存関係通りに完全実行されました。\n");
                                } else if !failed_tasks.is_empty() {
                                    println!("❌ [Ninja Engine] タスクの実行に失敗したため、後続処理を中断しました。失敗タスク: {:?}", failed_tasks);
                                } else {
                                    println!("🛑 [Ninja Engine] 致命的エラー: デッドロック（循環依存または未定義の依存関係）を検出しました。");
                                    println!("  └── 実行不可能なタスク群: {:?}", pending_tasks);
                                    println!();
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