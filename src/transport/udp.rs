// src/transport/udp.rs

use anyhow::Result;
use tokio::net::UdpSocket;

pub struct UdpTransport {
    socket: UdpSocket,
}

impl UdpTransport {
    pub async fn bind(addr: &str) -> Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self { socket })
    }

    pub async fn send_to(&self, buf: &[u8], target: &str) -> Result<usize> {
        let sent = self.socket.send_to(buf, target).await?;
        Ok(sent)
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, std::net::SocketAddr)> {
        let (len, addr) = self.socket.recv_from(buf).await?;
        Ok((len, addr))
    }
}