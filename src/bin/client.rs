// src/bin/client.rs
use ninja::core::packet::{NinjaPacket, FLAG_SYN, FLAG_DATA};
use ninja::core::path::{PathHeader, HopField};
use std::env;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

// グラフモジュールのTask構造体を再利用
#[path = "../core/graph.rs"]
pub mod graph;
use graph::Task;

fn main() {
    // 期待する引数形式: cargo run --bin client -- [自分のポート] [宛先IP:PORT] [ASパス...]
    // 例: cargo run --bin client -- 9000 127.0.0.1:4000 100 2 200 0
    let args: Vec<String> = env::args().collect();

    if args.len() < 4 {
        println!("Usage: cargo run --bin client -- [my_port] [dest_ip:port] [as_id egress_if ...]");
        println!("Example: cargo run --bin client -- 9000 127.0.0.1:4000 100 2 200 0");
        return;
    }

    let my_port = args[1].parse::<u16>().expect("Invalid client port");
    let dest_addr: SocketAddr = args[2].parse().expect("Invalid destination address");

    // 残りの引数から動的にホップフィールドを組み立てる
    let path_args = &args[3..];
    if path_args.len() % 2 != 0 {
        println!("Error: Path arguments must be pairs of (AS_ID, EGRESS_IF)");
        return;
    }

    let mut hops = Vec::new();
    for i in (0..path_args.len()).step_by(2) {
        let as_id = path_args[i].parse::<u32>().expect("Invalid AS ID in path");
        let egress_if_val = path_args[i+1].parse::<u16>().expect("Invalid Egress IF in path");
        
        // エラー修正: mac のサイズを 4バイト に合わせる
        hops.push(HopField { 
            as_id, 
            egress_if: egress_if_val,
            mac: [0u8; 4], // 4バイトのダミーMACを設定
        });
    }

    let path_header = PathHeader {
        current_hop_index: 0,
        hops,
    };

    println!("=============================================");
    println!("  ninja client starting on port {}...", my_port);
    println!("  Targeting Router: {}", dest_addr);
    println!("  Configured Path: {:?}", path_header.hops);
    println!("=============================================");

    // クライアント自身の真のポートでUDPバインド（応答受け取り用）
    let bind_addr = format!("127.0.0.1:{}", my_port);
    let socket = UdpSocket::bind(&bind_addr).expect("Failed to bind local UDP socket");
    socket.set_read_timeout(Some(Duration::from_secs(3))).expect("Failed to set timeout");

    // 1. まずはコネクション確立（SYN）を送信
    let mut syn_payload = Vec::new();
    syn_payload.extend_from_slice(&my_port.to_be_bytes()); // 先頭2バイトに真のポートを隠す
    syn_payload.extend_from_slice(b"Hello Control Plane");

    let syn_packet = NinjaPacket::new(FLAG_SYN, Some(path_header.clone()), syn_payload);
    
    println!("[Client] Sending SYN packet to establish channel...");
    socket.send_to(&syn_packet.to_bytes(), dest_addr).expect("Failed to send SYN");

    // 応答待受
    let mut buf = [0u8; 2048];
    match socket.recv_from(&mut buf) {
        Ok((size, from)) => {
            if let Ok(ack_packet) = NinjaPacket::from_bytes(&buf[..size]) {
                if ack_packet.is_ack() {
                    println!("🎉 [Client] Handshake Success! Received response from {}: {}", from, String::from_utf8_lossy(&ack_packet.payload));
                    
                    // --------------------------------------------------------
                    // 依存関係タスクグラフ（Graph）を送信する
                    // --------------------------------------------------------
                    println!("\n[Client] 依存関係タスク（Graph）を構築中...");
                    let tasks = vec![
                        Task {
                            name: "deploy".to_string(),
                            deps: vec!["build".to_string(), "test".to_string()],
                            command: "echo 'Deploying to network...'".to_string(),
                        },
                        Task {
                            name: "build".to_string(),
                            deps: vec!["codegen".to_string()],
                            command: "cargo build".to_string(),
                        },
                        Task {
                            name: "test".to_string(),
                            deps: vec!["codegen".to_string()],
                            command: "cargo test".to_string(),
                        },
                        Task {
                            name: "codegen".to_string(),
                            deps: vec![],
                            command: "echo 'Generating path matrices...'".to_string(),
                        },
                    ];

                    // タスク配列をJSON文字列にシリアライズ
                    match serde_json::to_vec(&tasks) {
                        Ok(json_payload) => {
                            println!("[Client] Sending Task Graph (DATA packet, size: {} bytes)...", json_payload.len());
                            let mut fresh_path = path_header.clone();
                            fresh_path.current_hop_index = 0;

                            let data_packet = NinjaPacket::new(FLAG_DATA, Some(fresh_path), json_payload);
                            socket.send_to(&data_packet.to_bytes(), dest_addr).expect("Failed to send DATA");
                            println!("[Client] Task Graph sent successfully.");
                        }
                        Err(e) => {
                            println!("[Client] Failed to serialize tasks: {}", e);
                        }
                    }
                    // --------------------------------------------------------
                }
            }
        }
        Err(e) => {
            println!("[Client] Timeout or error waiting for ACK: {:?}", e);
        }
    }
}