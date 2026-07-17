use crate::platform::abstraction::Transport;
use std::net::{UdpSocket, SocketAddr};

pub struct UdpTransport {
    socket: UdpSocket,
}

impl UdpTransport {
    pub fn bind(addr: &str) -> Self {
        let socket = UdpSocket::bind(addr).expect("Failed to bind UDP socket");
        Self { socket }
    }
}

impl Transport for UdpTransport {
    fn send(&self, data: &[u8], to: SocketAddr) {
        // 送信時のエラーもパニックさせずにログ出力にとどめる
        if let Err(e) = self.socket.send_to(data, to) {
            println!("[Transport Error] Failed to send packet: {:?}", e);
        }
    }

    fn recv(&self) -> (Vec<u8>, SocketAddr) {
        let mut buf = [0u8; 65535];
        
        loop {
            // 【重要】recv_from が返すエラー（ConnectionResetなど）をループ内で適切に処理する
            match self.socket.recv_from(&mut buf) {
                Ok((size, addr)) => {
                    return (buf[..size].to_vec(), addr);
                }
                Err(e) => {
                    // Windowsでよく発生する 10054 (ConnectionReset) などのエラーを検知
                    // これらは無視して、次の正しいパケットの受信待ちに移行する
                    if e.kind() == std::io::ErrorKind::ConnectionReset {
                        // 転送先が存在しなかった時のICMPエラーの通知なので、無視してループを継続
                        continue;
                    }
                    
                    // それ以外の未知のエラーの場合も、サーバーを落とさずにログだけ吐いて継続
                    println!("[Transport Warning] recv_from encountered an error: {:?}", e);
                    continue;
                }
            }
        }
    }
}