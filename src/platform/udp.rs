use std::net::{UdpSocket, SocketAddr};
use crate::platform::abstraction::Transport;

pub struct UdpTransport {
    socket: UdpSocket,
}

impl UdpTransport {
    pub fn bind(addr: &str) -> Self {
        let socket = UdpSocket::bind(addr).expect("bind failed");
        Self { socket }
    }
}

impl Transport for UdpTransport {
    fn recv(&self) -> (Vec<u8>, SocketAddr) {
        let mut buf = [0u8; 1024];
        let (size, src) = self.socket.recv_from(&mut buf).unwrap();
        (buf[..size].to_vec(), src)
    }

    fn send(&self, data: &[u8], addr: SocketAddr) {
        self.socket.send_to(data, addr).unwrap();
    }
}