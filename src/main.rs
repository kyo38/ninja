use ninja::platform::udp::UdpTransport;
use ninja::platform::abstraction::Transport;
use ninja::core::packet::{NinjaPacket, FLAG_ACK};
use std::collections::HashMap;
use std::net::SocketAddr;

fn main() {
    println!("ninja router/server starting on AS 100...");

    let my_as_id: u32 = 100;

    let mut topology = HashMap::new();
    topology.insert(2, "127.0.0.1:5000".to_string());
    topology.insert(3, "127.0.0.1:6000".to_string());

    let transport = UdpTransport::bind("127.0.0.1:4000");

    loop {
        let (data, addr) = transport.recv();

        match NinjaPacket::from_bytes(&data) {
            Ok(mut packet) => {
                let mut should_process_locally = true;

                if let Some(ref mut path_header) = packet.path {
                    println!("Received packet. Current Hop Index: {}", path_header.current_hop_index);
                    
                    if let Some(current_hop) = path_header.current_hop() {
                        if current_hop.as_id != my_as_id {
                            println!("Path Error: Packet expected AS {}, but reached AS {}", current_hop.as_id, my_as_id);
                            continue;
                        }

                        let egress_if = current_hop.egress_if;
                        let is_over = path_header.increment_hop();

                        if !is_over && egress_if != 0 {
                            should_process_locally = false;

                            if let Some(next_hop_str) = topology.get(&egress_if) {
                                match next_hop_str.parse::<SocketAddr>() {
                                    Ok(next_hop_ip) => {
                                        println!("--> Forwarding packet via Interface {} to {}", egress_if, next_hop_ip);
                                        // 【修正】send自体がパニックする可能性がある場合を考慮し、
                                        // 独自実装のTransportトレイトの制約内で安全に処理する
                                        transport.send(&packet.to_bytes(), next_hop_ip);
                                    }
                                    Err(_) => {
                                        println!("Topology Error: Invalid address: {}", next_hop_str);
                                    }
                                }
                            } else {
                                println!("Routing Error: No topology mapping for Interface {}", egress_if);
                            }
                        }
                    }
                }

                if should_process_locally {
                    if packet.is_syn() {
                        println!("Reached Final Destination! SYN from {}", addr);
                        let ack = NinjaPacket::new(FLAG_ACK, None, b"syn-ack".to_vec());
                        transport.send(&ack.to_bytes(), addr);
                    } else if packet.is_data() {
                        println!("Reached Final Destination! DATA from {}: {}", addr, String::from_utf8_lossy(&packet.payload));
                    }
                }
            }
            Err(e) => {
                println!("Invalid packet from {}: {:?}", addr, e);
            }
        }
    }
}