use std::net::SocketAddr;

pub trait Transport {
    fn recv(&self) -> (Vec<u8>, SocketAddr);
    fn send(&self, data: &[u8], addr: SocketAddr);
}