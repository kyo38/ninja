use ninja::core::packet::{NinjaPacket, FLAG_SYN};
use ninja::core::path::{PathHeader, HopField};
use rand::seq::SliceRandom;
use std::env;
use std::net::UdpSocket;
use std::thread;
use std::time::Duration;

#[derive(Clone)]
struct ClientHop {
    as_id: u32,
    egress_if: u16,
}

struct PathGenerator {
    hops: Vec<ClientHop>,
}

fn main() {
    // コマンドライン引数をパース（デフォルト値: AS 200, ポート 5000）
    let args: Vec<String> = env::args().collect();
    let target_as: u32 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(200);
    let target_port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5000);

    println!("=============================================");
    println!("  Client started with Multi-hop Path-Switching!");
    println!("  Targeting Entry Node: AS {} @ 127.0.0.1:{}", target_as, target_port);
    println!("=============================================");

    let socket = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind socket");
    socket.set_read_timeout(Some(Duration::from_secs(2))).expect("Failed to set timeout");

    let mut rng = rand::thread_rng();

    for i in 1..=5 {
        println!("\n--- [Test Round {}] ---", i);

        // クロージャを使わず、ループの中で毎回動的なパスの選択肢を配列として生成します
        let mut paths = vec![
            // パスA: AS target_as(IF 2) -> 次のAS(IF 0)
            PathGenerator {
                hops: vec![
                    ClientHop { as_id: target_as, egress_if: 2 },
                    ClientHop { as_id: target_as + 100, egress_if: 0 },
                ],
            },
            // パスB: AS target_as(IF 3) -> 次のAS(IF 0)
            PathGenerator {
                hops: vec![
                    ClientHop { as_id: target_as, egress_if: 3 },
                    ClientHop { as_id: target_as + 200, egress_if: 0 },
                ],
            },
            // パスC: AS target_as(IF 0: ローカル終了)
            PathGenerator {
                hops: vec![
                    ClientHop { as_id: target_as, egress_if: 0 },
                ],
            },
        ];

        // 経路をランダムにシャッフルして選択
        paths.shuffle(&mut rng);
        let selected_path = &paths[0];

        print!("-> Selected Path: ");
        for (idx, hop) in selected_path.hops.iter().enumerate() {
            if idx > 0 { print!(" -> "); }
            print!("AS {}(IF {})", hop.as_id, hop.egress_if);
        }
        println!();

        let my_port = socket.local_addr().unwrap().port();
        let mut payload = Vec::new();
        payload.extend_from_slice(&my_port.to_be_bytes());
        payload.extend_from_slice(b"hello SCION multipath");

        let mut router_hops = Vec::new();
        for hop in &selected_path.hops {
            let hop_field = HopField {
                as_id: hop.as_id,
                egress_if: hop.egress_if,
                mac: [0; 4],
            };
            router_hops.push(hop_field);
        }

        let path_header = PathHeader {
            current_hop_index: 0,
            hops: router_hops,
        };

        let packet = NinjaPacket::new(FLAG_SYN, Some(path_header), payload);
        let bytes = packet.to_bytes();

        // 指定されたポートへ動的に送信
        let target_addr = format!("127.0.0.1:{}", target_port);
        socket.send_to(&bytes, &target_addr).expect("Failed to send");
        println!("Packet injected to Entry Node (AS {} @ {}).", target_as, target_addr);

        let mut buf = [0u8; 1024];
        println!("Waiting for response...");
        
        let mut retry_count = 0;
        loop {
            match socket.recv_from(&mut buf) {
                Ok((size, addr)) => {
                    println!("🎉 Response received from {}: {:?}", addr, &buf[..size]);
                    break;
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::ConnectionReset && retry_count < 2 {
                        retry_count += 1;
                        continue;
                    }
                    if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut {
                        println!("[Info] Timeout - No response (Path might be broken or target node offline).");
                    } else {
                        println!("[Error] Communication error: {:?}", e);
                    }
                    break;
                }
            }
        }

        thread::sleep(Duration::from_millis(800));
    }
}