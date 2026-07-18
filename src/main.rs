// src/main.rs
pub mod core {
    pub mod graph;
}
use ninja::platform::udp::UdpTransport;
use ninja::platform::abstraction::Transport;
use ninja::core::packet::{NinjaPacket, FLAG_ACK};
use core::graph::{Task, resolve_execution_order};
use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;

// タスクの実行状態を管理する列挙型
#[derive(Clone, Copy, Debug, PartialEq)]
enum TaskStatus {
    Pending,
    Done,
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

    // 起動時セルフテスト（整合性チェックは残す）
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
                                
                                // 1. 事前の循環参照チェック（防衛線）
                                if let Err(graph_err) = resolve_execution_order(&received_tasks) {
                                    eprintln!("[AS {}] ✗ 処理不可能なグラフです: {}", my_as_id, graph_err);
                                    continue;
                                }

                                // 2. エンジンの状態管理（State）を初期化
                                let mut task_states: HashMap<String, TaskStatus> = received_tasks
                                    .iter()
                                    .map(|t| (t.name.clone(), TaskStatus::Pending))
                                    .collect();

                                println!("🚀 [Ninja Engine] --- スケジューリングループ開始 ---");

                                // 3. 真のDAGスケジュールループ
                                loop {
                                    let mut progress = false;
                                    let mut ready_tasks = Vec::new();

                                    // 現在実行可能なタスク（Pendingかつ、すべての依存タスクがDone）をスキャン
                                    for task in &received_tasks {
                                        if task_states.get(&task.name) == Some(&TaskStatus::Done) {
                                            continue;
                                        }

                                        // すべての依存先（deps）がDoneになっているか判定
                                        let is_ready = task.deps.iter().all(|dep| {
                                            task_states.get(dep) == Some(&TaskStatus::Done)
                                        });

                                        if is_ready {
                                            ready_tasks.push(task.clone());
                                        }
                                    }

                                    // 実行可能タスクがある場合、それを実行（本来ここは並列化できるポイント）
                                    if !ready_tasks.is_empty() {
                                        // 並列性の可視化のために、同時に実行可能になったタスク群を表示
                                        if ready_tasks.len() > 1 {
                                            let names: Vec<String> = ready_tasks.iter().map(|t| t.name.clone()).collect();
                                            println!("  [⚡ Parallel Ready] 同時実行可能なタスク群を検知: {:?}", names);
                                        }

                                        for task in ready_tasks {
                                            println!("  ⚡ [Execute] ➔ [{}] Running: {}", task.name, task.command);
                                            
                                            // ここでタスクの状態を更新（Doneへ遷移）
                                            task_states.insert(task.name.clone(), TaskStatus::Done);
                                            progress = true;
                                        }
                                    }

                                    // 1周の間で1つも進捗がなければ、すべての依存が解決したか、あるいはデッドロック
                                    if !progress {
                                        break;
                                    }
                                }

                                // 最終状態の確認
                                let all_done = task_states.values().all(|s| *s == TaskStatus::Done);
                                if all_done {
                                    println!("🎉 [Ninja Engine] 全てのタスクグラフが依存関係通りに完全実行されました。\n");
                                } else {
                                    println!("🛑 [Ninja Engine] 未解決のタスクが残っています（デッドロックの可能性）。\n");
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