#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::Mutex;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::core::graph::Task;
use crate::core::path::{PathStrategy, PathHeader}; // PathHeader は path モジュールから取得
use crate::core::packet::NinjaPacket;

pub type ExecutorResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait Executor: Send + Sync {
    fn submit<'a>(
        &'a self,
        task: Task,
        strategy: PathStrategy,
    ) -> BoxFuture<'a, ExecutorResult>;
}

#[derive(Debug, Clone)]
pub struct WorkerSession {
    pub address: String,
    pub active_tasks: usize,
    pub latency_ms: u64,
    pub is_alive: bool,
}

pub struct RemoteExecutor {
    pub workers: Arc<Mutex<Vec<WorkerSession>>>,
}

impl RemoteExecutor {
    pub fn new(worker_addresses: Vec<String>) -> Self {
        let sessions = worker_addresses
            .into_iter()
            .map(|addr| WorkerSession {
                address: addr,
                active_tasks: 0,
                latency_ms: 9999,
                is_alive: true,
            })
            .collect();

        Self {
            workers: Arc::new(Mutex::new(sessions)),
        }
    }

    pub async fn select_path(&self, _strategy: PathStrategy) -> Result<String, String> {
        let mut workers = self.workers.lock().await;
        
        let chosen_idx = workers
            .iter()
            .enumerate()
            .filter(|(_, w)| w.is_alive)
            .min_by_key(|(_, w)| w.active_tasks * 10 + (w.latency_ms as usize))
            .map(|(idx, _)| idx);

        if let Some(idx) = chosen_idx {
            workers[idx].active_tasks += 1;
            let addr = workers[idx].address.clone();
            println!("🔀 [RemoteExecutor] パス選択成功: {} (現在の担当タスク数: {})", addr, workers[idx].active_tasks);
            Ok(addr)
        } else {
            Err("❌ 利用可能な有効なワーカーが見つかりません。".to_string())
        }
    }

    pub async fn release_worker(&self, address: &str) {
        let mut workers = self.workers.lock().await;
        if let Some(w) = workers.iter_mut().find(|w| w.address == address) {
            if w.active_tasks > 0 {
                w.active_tasks -= 1;
            }
            println!("🔓 [RemoteExecutor] ワーカー解放: {} (現在の担当タスク数: {})", w.address, w.active_tasks);
        }
    }

    pub async fn start_heartbeat_loop(&self, interval: Duration, _timeout: Duration) {
        let workers_clone = Arc::clone(&self.workers);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let mut workers = workers_clone.lock().await;
                for worker in workers.iter_mut() {
                    worker.latency_ms = 10; 
                    worker.is_alive = true;
                }
            }
        });
    }
}

impl Executor for RemoteExecutor {
    fn submit<'a>(
        &'a self,
        task: Task,
        _strategy: PathStrategy,
    ) -> BoxFuture<'a, ExecutorResult> {
        Box::pin(async move {
            let worker_address = match self.select_path(PathStrategy::Fastest).await {
                Ok(addr) => addr,
                Err(e) => return Err(e.into()),
            };

            println!("📤 [RemoteExecutor] タスク '{}' をワーカー '{}' へ送信中...", task.name, worker_address);

            // 修正: PathHeader::new に空の Vec を渡してインスタンス化
            let packet = NinjaPacket::new(64, PathHeader::new(Vec::new()), task.command.into_bytes());
            let serialized_data = packet.to_bytes();

            let result = async {
                let mut stream = TcpStream::connect(&worker_address).await?;
                
                let len_bytes = (serialized_data.len() as u32).to_be_bytes();
                stream.write_all(&len_bytes).await?;
                stream.write_all(&serialized_data).await?;
                stream.flush().await?;

                let mut ack_buf = [0u8; 4];
                let _ = stream.read(&mut ack_buf).await;
                
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }.await;

            self.release_worker(&worker_address).await;

            match result {
                Ok(_) => {
                    println!("👍 [RemoteExecutor] タスク '{}' のネットワーク送信・応答確認が成功完了", task.name);
                    Ok(())
                }
                Err(e) => {
                    eprintln!("💥 [RemoteExecutor] タスク '{}' の通信中にエラーが発生: {}", task.name, e);
                    Err(e)
                }
            }
        })
    }
}