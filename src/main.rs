use ninja::platform::udp::UdpTransport;
use ninja::platform::abstraction::Transport;
use ninja::core::packet::{NinjaPacket, FLAG_ACK};

fn main() {
    println!("ninja starting...");

    let transport = UdpTransport::bind("127.0.0.1:4000");

    loop {
        let (data, addr) = transport.recv();

        match NinjaPacket::from_bytes(&data) {
            Ok(packet) => {
                if packet.is_syn() {
                    println!("SYN from {}", addr);

                    let ack = NinjaPacket::new(FLAG_ACK, b"syn-ack".to_vec());
                    transport.send(&ack.to_bytes(), addr);
                } else if packet.is_data() {
                    println!(
                        "DATA from {}: {}",
                        addr,
                        String::from_utf8_lossy(&packet.payload)
                    );
                }
            }
            Err(e) => {
                println!("Invalid packet from {}: {:?}", addr, e);
            }
        }
    }
}