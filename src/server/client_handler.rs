// src/server/client_handler.rs

use tokio::net::TcpListener;
use tokio::io::AsyncReadExt;
use tracing::info;

use crate::core::graph::Task;
use crate::core::error::NinjaError;

pub struct ClientHandler {
    listener: TcpListener,
}

impl ClientHandler {
    pub async fn bind(addr: &str) -> Result<Self, NinjaError> {
        let listener = TcpListener::bind(addr).await?;
        info!("📡 [Master] クライアントからのDAGタスク投入を待機中... (ポート: {})", addr);
        Ok(Self { listener })
    }

    pub async fn accept_tasks(&mut self) -> Result<Vec<Task>, NinjaError> {
        let (mut stream, addr) = self.listener.accept().await?;
        info!("📥 [Master] クライアントから接続されました: {}", addr);

        let mut buffer = vec![0; 65536];
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            return Err(NinjaError::NetworkError("空のデータを受信しました".into()));
        }

        let json_str = std::str::from_utf8(&buffer[..n])
            .map_err(|e| NinjaError::NetworkError(e.to_string()))?;
        let tasks: Vec<Task> = serde_json::from_str(json_str)?;
        
        info!("📦 正常に {} つのタスクを含むDAGを受信しました。", tasks.len());
        Ok(tasks)
    }
}