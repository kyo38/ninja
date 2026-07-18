use ninja::platform::udp::UdpTransport;
use ninja::platform::abstraction::Transport;
use ninja::core::packet::{NinjaPacket, FLAG_ACK};
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;

fn main() {
    // コマンドライン引数の処理
    // 期待する形式: cargo run --bin ninja -- [AS番号] [待ち受けポート]
    // 例: cargo run --bin ninja -- 100 4000
    let args: Vec<String> = env::args().collect();
    
    let (my_as_id, listen_port) = if args.len() >= 3 {
        let as_id = args[1].parse::<u32>().expect("Invalid AS ID");
        let port = args[2].parse::<u16>().expect("Invalid Port");
        (as_id, port)
    } else {
        // 引数がない場合はデフォルトで AS 100 / ポート 4000 として動かす
        (100, 4000)
    };

    println!("=============================================");
    println!("  ninja router/server starting on AS {}...", my_as_id);
    println!("  Listening on 127.0.0.1:{}", listen_port);
    println!("=============================================");

    // 各ルーターのトポロジーマップ（インターフェース ⇄ 次のルーターのIP:PORT）
    let mut topology = HashMap::new();
    
    // 全ノード共通の簡易的な静的ルーティングテーブル
    topology.insert(2, "127.0.0.1:5000".to_string()); // 2番IFは AS 200 (ポート5000) へ
    topology.insert(3, "127.0.0.1:6000".to_string()); // 3番IFは AS 300 (ポート6000) へ

    let bind_addr = format!("127.0.0.1:{}", listen_port);
    let transport = UdpTransport::bind(&bind_addr);

    loop {
        let (data, addr) = transport.recv();

        match NinjaPacket::from_bytes(&data) {
            Ok(mut packet) => {
                let mut should_process_locally = true;

                if let Some(ref mut path_header) = packet.path {
                    println!("[AS {}] Received packet. Current Hop Index: {}", my_as_id, path_header.current_hop_index);
                    
                    if let Some(current_hop) = path_header.current_hop() {
                        // 1. 自分が処理すべき正しいASかチェック
                        if current_hop.as_id != my_as_id {
                            println!("[AS {}] Path Error: Packet expected AS {}, but reached AS {}", my_as_id, current_hop.as_id, my_as_id);
                            continue;
                        }

                        let egress_if = current_hop.egress_if;
                        
                        // 2. ホップをインクリメント（次のノードへポインタを進める）
                        let is_over = path_header.increment_hop();

                        // 3. まだ経路（ホップ）が残っており、かつ出力インターフェースが0（ローカル終了）でなければ転送
                        if !is_over && egress_if != 0 {
                            should_process_locally = false; // 転送するためローカル処理はスキップ

                            if let Some(next_hop_str) = topology.get(&egress_if) {
                                match next_hop_str.parse::<SocketAddr>() {
                                    Ok(next_hop_ip) => {
                                        println!("[AS {}] --> Forwarding packet via Interface {} to {}", my_as_id, egress_if, next_hop_ip);
                                        // 次のルーターへバケツリレー
                                        transport.send(&packet.to_bytes(), next_hop_ip);
                                    }
                                    Err(_) => {
                                        println!("[AS {}] Topology Error: Invalid address: {}", my_as_id, next_hop_str);
                                    }
                                }
                            } else {
                                println!("[AS {}] Routing Error: No topology mapping for Interface {}", my_as_id, egress_if);
                            }
                        }
                    }
                }

                // 最終目的地（またはパスヘッダなし）の場合の処理
                if should_process_locally {
                    if packet.is_syn() {
                        // ペイロードの先頭2バイトからクライアントの真のポート番号を復元
                        if packet.payload.len() >= 2 {
                            let port_bytes = [packet.payload[0], packet.payload[1]];
                            let client_port = u16::from_be_bytes(port_bytes);
                            let real_client_addr = SocketAddr::new(addr.ip(), client_port);

                            println!("🎉 [AS {}] Reached Final Destination! SYN received. Original Client: {}", my_as_id, real_client_addr);
                            
                            // 本当のクライアントのポートへ直接応答を返す
                            let ack = NinjaPacket::new(FLAG_ACK, None, b"syn-ack".to_vec());
                            transport.send(&ack.to_bytes(), real_client_addr);
                        } else {
                            println!("🎉 [AS {}] Reached Final Destination! (Payload too short to extract port)", my_as_id);
                        }
                    } else if packet.is_data() {
                        println!("🎉 [AS {}] Reached Final Destination! DATA: {}", my_as_id, String::from_utf8_lossy(&packet.payload));
                    }
                }
            }
            Err(e) => {
                println!("[AS {}] Invalid packet from {}: {:?}", my_as_id, addr, e);
            }
        }
    }
}