// src/server/worker_pool.rs

use std::error::Error;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify};

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

    /// Worker受付用サーバー (Port 9001) のバックグラウンド起動
    pub async fn start_listener(&self, addr: &str) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind(addr).await?;
        println!("📡 [Master] Workerからの接続を待機中... (ポート: {})", addr);

        let workers_clone = Arc::clone(&self.workers);
        let pulse_clone = Arc::clone(&self.pulse);

        tokio::spawn(async move {
            let mut id_counter = 0;
            loop {
                if let Ok((stream, client_addr)) = listener.accept().await {
                    id_counter += 1;
                    println!("🤝 [Master] Workerがクラスタに参加しました: {} (ID: {})", client_addr, id_counter);

                    let mut list = workers_clone.lock().await;
                    list.push(WorkerSession { id: id_counter, stream });

                    // 新しいWorkerが参入したことを通知
                    pulse_clone.notify_waiters();
                }
            }
        });

        Ok(())
    }

    pub fn get_inner(&self) -> Arc<Mutex<Vec<WorkerSession>>> {
        Arc::clone(&self.workers)
    }
}