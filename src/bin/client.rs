use std::net::UdpSocket;
use std::time::Duration;
use std::thread;
use ninja::core::packet::{NinjaPacket, FLAG_SYN};
use ninja::core::path::{PathHeader, HopField};
use rand::seq::SliceRandom;

fn path_option_a() -> PathHeader {
    PathHeader::new(vec![
        HopField { as_id: 100, egress_if: 2, mac: [1, 2, 3, 4] },
        HopField { as_id: 200, egress_if: 0, mac: [5, 6, 7, 8] },
    ])
}

fn path_option_b() -> PathHeader {
    PathHeader::new(vec![
        HopField { as_id: 100, egress_if: 3, mac: [11, 12, 13, 14] },
        HopField { as_id: 300, egress_if: 0, mac: [15, 16, 17, 18] },
    ])
}

fn path_option_c() -> PathHeader {
    PathHeader::new(vec![
        HopField { as_id: 100, egress_if: 9, mac: [99, 99, 99, 99] },
    ])
}

fn main() {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("Failed to bind");
    socket.set_read_timeout(Some(Duration::from_secs(1))).expect("Failed to set timeout");

    println!("Client started with Path-Switching feature!");

    let mut path_generators: [fn() -> PathHeader; 3] = [
        path_option_a,
        path_option_b,
        path_option_c,
    ];

    let mut rng = rand::thread_rng();

    // 5回連続でランダムにパスを変えて送信してみる
    for i in 1..=5 {
        println!("\n--- [Test Round {}] ---", i);

        // 配列をシャッフル
        path_generators.shuffle(&mut rng);
        let selected_path = path_generators[0]();

        if let Some(first_hop) = selected_path.hops.first() {
            println!("-> Selected Path Target: AS {}, via Interface {}", first_hop.as_id, first_hop.egress_if);
        }

        let packet = NinjaPacket::new(FLAG_SYN, Some(selected_path), b"hello SCION multipath".to_vec());
        let bytes = packet.to_bytes();

        socket.send_to(&bytes, "127.0.0.1:4000").expect("Failed to send");
        println!("Packet sent to 127.0.0.1:4000.");

        let mut buf = [0u8; 1024];
        match socket.recv_from(&mut buf) {
            Ok((size, _)) => {
                println!("Response received: {:?}", &buf[..size]);
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut {
                    println!("[Info] Timeout - No active node ahead on this path.");
                } else {
                    println!("[Error] Communication error: {:?}", e);
                }
            }
        }

        // ログを見やすくするために少しだけ待つ
        thread::sleep(Duration::from_millis(500));
    }
}