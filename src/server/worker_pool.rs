// src/server/worker_pool.rs

use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify};
use tracing::info;
use crate::core::error::NinjaError;

pub struct WorkerSession {
    pub id: usize,
    pub stream: TcpStream,
}

#[derive(Clone)]
pub struct WorkerPool {
    workers: Arc<Mutex<Vec<WorkerSession>>>,
    pulse: Arc<Notify>,
}

impl WorkerPool {
    pub fn new(pulse: Arc<Notify>) -> Self {
        Self {
            workers: Arc::new(Mutex::new(Vec::new())),
            pulse,
        }
    }

    pub async fn start_listener(&self, addr: &str) -> Result<(), NinjaError> {
        let listener = TcpListener::bind(addr).await?;
        info!("📡 [Master] Workerからの接続を待機中... (ポート: {})", addr);

        let workers_clone = Arc::clone(&self.workers);
        let pulse_clone = Arc::clone(&self.pulse);

        tokio::spawn(async move {
            let mut id_counter = 0;
            loop {
                if let Ok((stream, client_addr)) = listener.accept().await {
                    id_counter += 1;
                    info!("🤝 [Master] Workerがクラスタに参加しました: {} (ID: {})", client_addr, id_counter);

                    let mut list = workers_clone.lock().await;
                    list.push(WorkerSession { id: id_counter, stream });

                    pulse_clone.notify_waiters();
                }
            }
        });

        Ok(())
    }

    /// 接続中の WorkerSession から 1 つを取得し、コマンドを送信してレスポンスを受け取る
    pub async fn send_command(&self, command: &str) -> Result<String, NinjaError> {
        let mut list = self.workers.lock().await;
        if list.is_empty() {
            return Err(NinjaError::WorkerError("利用可能な Worker 接続がありません。".into()));
        }

        // 先頭の WorkerSession を利用して通信
        let session = &mut list[0];
        let (reader, mut writer) = session.stream.split();
        let mut buf_reader = BufReader::new(reader);

        // 改行付きでコマンドを書き込み
        let payload = format!("{}\n", command);
        writer.write_all(payload.as_bytes()).await?;
        writer.flush().await?;

        // Worker からのレスポンス（1行）を読み込み
        let mut response = String::new();
        buf_reader.read_line(&mut response).await?;

        Ok(response.trim().to_string())
    }

    pub fn get_inner(&self) -> Arc<Mutex<Vec<WorkerSession>>> {
        Arc::clone(&self.workers)
    }
}