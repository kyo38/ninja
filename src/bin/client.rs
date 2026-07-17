use std::net::UdpSocket;
use ninja::core::packet::{NinjaPacket, FLAG_SYN};

fn main() {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

    // SYN送信
    let packet = NinjaPacket::new(FLAG_SYN, b"hello".to_vec());
    let bytes = packet.to_bytes();

    socket.send_to(&bytes, "127.0.0.1:4000").unwrap();

    let mut buf = [0u8; 1024];
    let (size, _) = socket.recv_from(&mut buf).unwrap();

    println!("response: {:?}", &buf[..size]);
}