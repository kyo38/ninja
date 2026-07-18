// src/main.rs
pub mod core {
    pub mod graph;
}
use ninja::platform::udp::UdpTransport;
use ninja::platform::abstraction::Transport;
use ninja::core::packet::{NinjaPacket, FLAG_ACK};
use core::graph::{Task, resolve_execution_order, Executor, LocalExecutor};
use std::collections::{HashMap, VecDeque};
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;

fn main() {
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
                                // 🛠️ 1. イニシャルリセット（データ構造の構築）
                                // =========================================================
                                let mut indegrees = HashMap::new();
                                let mut adjacency_list = HashMap::new();
                                let mut task_map = HashMap::new();

                                // タスク名から実体を引くマップと、隣接リストの初期化
                                for task in &received_tasks {
                                    task_map.insert(task.name.clone(), task.clone());
                                    indegrees.insert(task.name.clone(), task.deps.len());
                                    adjacency_list.insert(task.name.clone(), Vec::new());
                                }

                                // 依存関係の逆引き（隣接リスト）を構築
                                // 例: codegen が終わったら -> [build, test] の indegree を減らす
                                for task in &received_tasks {
                                    for dep in &task.deps {
                                        if let Some(list) = adjacency_list.get_mut(dep) {
                                            list.push(task.name.clone());
                                        }
                                    }
                                }

                                // 💡 超重要：初期状態で依存ゼロ（indegree == 0）のタスクを Ready Queue に投入
                                let mut ready_queue = VecDeque::new();
                                for (name, &deg) in &indegrees {
                                    if deg == 0 {
                                        ready_queue.push_back(name.clone());
                                    }
                                }

                                let (tx, rx) = mpsc::channel::<(String, bool)>(); // (タスク名, 成功フラグ)
                                let mut running_count = 0;
                                let mut total_processed = 0;
                                let mut has_failed = false;

                                println!("🚀 [Ninja Engine] --- スケジューリングループ開始 ---");

                                // =========================================================
                                // 🔄 2. メインループ（Ready Queue とイベントカウンタによる純粋駆動）
                                // =========================================================
                                loop {
                                    // エラー発生時は新規タスクの起動をストップ
                                    if !has_failed {
                                        // 💡 Ready Queue にあるタスクを「あるだけ全部」一斉にスレッド起動（真の並列化）
                                        if ready_queue.len() > 1 {
                                            let names: Vec<String> = ready_queue.iter().cloned().collect();
                                            println!("  [⚡ Parallel Ready] 同時並列実行を開始します: {:?}", names);
                                        }

                                        while let Some(task_name) = ready_queue.pop_front() {
                                            let task = task_map.get(&task_name).unwrap().clone();
                                            let tx_clone = tx.clone();
                                            let exec_clone = Arc::clone(&executor);

                                            running_count += 1;

                                            thread::spawn(move || {
                                                let success = exec_clone.execute(&task);
                                                let _ = tx_clone.send((task.name, success));
                                            });
                                        }
                                    }

                                    // 終了判定：走っているスレッドがなく、Ready Queue も空
                                    if running_count == 0 {
                                        break;
                                    }

                                    // 💡 イベント待ち：Workerスレッドからの完了通知が届くまで「完全沈黙」
                                    if let Ok((finished_task, success)) = rx.recv() {
                                        running_count -= 1;
                                        total_processed += 1;

                                        if !success {
                                            has_failed = true;
                                            println!("❌ [Ninja Engine] タスク [{}] が失敗しました。後続の発火を停止します。", finished_task);
                                            continue;
                                        }

                                        // 💡 イベント駆動の核心：完了したタスクの「後続ノード」の indegree をデクリメント
                                        if let Some(followers) = adjacency_list.get(&finished_task) {
                                            for follower in followers {
                                                if let Some(deg) = indegrees.get_mut(follower) {
                                                    *deg -= 1;
                                                    // 依存数が 0 になった瞬間、即座に Ready Queue へ昇格！
                                                    if *deg == 0 {
                                                        ready_queue.push_back(follower.clone());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                // =========================================================
                                // 🏁 3. 終了処理（状態の判定）
                                // =========================================================
                                if has_failed {
                                    println!("❌ [Ninja Engine] 一部タスクのエラーにより、実行が中断されました。");
                                } else if total_processed < received_tasks.len() {
                                    // 全タスク数に満たないのにループが終わった＝グラフに未解消の依存（循環参照）がある
                                    println!("🛑 [Ninja Engine] 致命的エラー: デッドロックを検出しました。循環依存の可能性があります。");
                                    let unresolved: Vec<String> = indegrees.iter()
                                        .filter(|&(_, &deg)| deg > 0)
                                        .map(|(name, _)| name.clone())
                                        .collect();
                                    println!("  └── 実行不可能（依存未解消）なタスク群: {:?}", unresolved);
                                    println!();
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