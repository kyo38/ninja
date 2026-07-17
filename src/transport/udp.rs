use tokio::net::UdpSocket;
use anyhow::Result;

pub struct UdpTransport {
    socket: UdpSocket,
}

impl UdpTransport {
    pub async fn new(bind_addr: &str) -> Result<Self> {
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self { socket })
    }

    pub async fn send(&self, data: &[u8], addr: &str) -> Result<()> {
        self.socket.send_to(data, addr).await?;
        Ok(())
    }

    pub async fn recv(&self, buf: &mut [u8]) -> Result<(usize, String)> {
        let (size, addr) = self.socket.recv_from(buf).await?;
        Ok((size, addr.to_string()))
    }
}