// src/server/client_handler.rs

use std::error::Error;
use tokio::net::TcpListener;
use tokio::io::AsyncReadExt;
use crate::core::graph::Task; // 👈 ninja:: から crate:: へ修正

pub struct ClientHandler {
    listener: TcpListener,
}

impl ClientHandler {
    pub async fn bind(addr: &str) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind(addr).await?;
        println!("📡 [Master] クライアントからのDAGタスク投入を待機中... (ポート: {})", addr);
        Ok(Self { listener })
    }

    /// クライアントからの接続を受信し、パースされたTaskのリストを返す
    pub async fn accept_tasks(&mut self) -> Result<Vec<Task>, Box<dyn Error>> {
        let (mut stream, addr) = self.listener.accept().await?;
        println!("📥 [Master] クライアントから接続されました: {}", addr);

        let mut buffer = vec![0; 65536];
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            return Err("空のデータを受信しました".into());
        }

        let json_str = std::str::from_utf8(&buffer[..n])?;
        let tasks: Vec<Task> = serde_json::from_str(json_str)?;
        
        println!("📦 正常に {} つのタスクを含むDAGを受信しました。", tasks.len());
        Ok(tasks)
    }
}