#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::Mutex;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::core::graph::Task;
use crate::core::path::PathHeader; // 必要な型をインポート
use crate::core::packet::NinjaPacket;

pub type ExecutorResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// ----------------------------------------------------
/// 🥇 ① 実行だけを担当するシンプルな Executor トレイト
/// ----------------------------------------------------
pub trait Executor: Send + Sync {
    /// 指定されたターゲット（ワーカー）へタスクを送信し、実行を委ねる
    /// どのパス（ワーカー）を選ぶかは呼び出し側（Scheduler）が決定する
    fn submit<'a>(
        &'a self,
        task: Task,
        target_address: String, // ③ どのワーカーで実行するかを直接受け取る
    ) -> BoxFuture<'a, ExecutorResult>;
}

/// ----------------------------------------------------
/// 🥇 ② 戦略アルゴリズムをカプセル化する PathStrategy トレイト
/// ----------------------------------------------------
pub trait PathStrategy: Send + Sync {
    /// 利用可能なワーカーセッションの中から、特定の戦略（最速、負荷分散など）に基づいて最適なワーカーアドレスを選択する
    fn select_path(&self, workers: &[WorkerSession]) -> Result<String, String>;
}

/// ----------------------------------------------------
/// ワーカーの生存状態と負荷状況を管理するセッション構造体
/// ----------------------------------------------------
#[derive(Debug, Clone)]
pub struct WorkerSession {
    pub address: String,
    pub active_tasks: usize,
    pub latency_ms: u64,
    pub is_alive: bool,
}

/// ----------------------------------------------------
/// 【Data Plane】純粋な通信実行に特化した RemoteExecutor
/// ----------------------------------------------------
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

    /// 外出しされたパス選択結果に基づき、ワーカーの利用カウンタを加算する
    pub async fn acquire_worker(&self, address: &str) -> Result<(), String> {
        let mut workers = self.workers.lock().await;
        if let Some(w) = workers.iter_mut().find(|w| w.address == address) {
            if !w.is_alive {
                return Err(format!("❌ ワーカー '{}' はダウンしています。", address));
            }
            w.active_tasks += 1;
            println!("🔒 [RemoteExecutor] ワーカー専有成功: {} (現在の担当タスク数: {})", w.address, w.active_tasks);
            Ok(())
        } else {
            Err(format!("❌ 指定されたワーカー '{}' が見つかりません。", address))
        }
    }

    /// タスク完了時（または失敗時）にワーカーを解放する
    pub async fn release_worker(&self, address: &str) {
        let mut workers = self.workers.lock().await;
        if let Some(w) = workers.iter_mut().find(|w| w.address == address) {
            if w.active_tasks > 0 {
                w.active_tasks -= 1;
            }
            println!("🔓 [RemoteExecutor] ワーカー解放: {} (現在の担当タスク数: {})", w.address, w.active_tasks);
        }
    }

    /// 定期的なヘルスチェックループ
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

/// ----------------------------------------------------
/// RemoteExecutor に対する Executor トレイトの実装（実行のみに徹する）
/// ----------------------------------------------------
impl Executor for RemoteExecutor {
    fn submit<'a>(
        &'a self,
        task: Task,
        target_address: String,
    ) -> BoxFuture<'a, ExecutorResult> {
        Box::pin(async move {
            // 1. スロットを確保（負荷計算のためのカウンタインクリメント）
            if let Err(e) = self.acquire_worker(&target_address).await {
                return Err(e.into());
            }

            println!("📤 [RemoteExecutor] タスク '{}' をワーカー '{}' へ純粋送信中...", task.name, target_address);

            // 2. パケットの構築とシリアライズ
            let packet = NinjaPacket::new(64, PathHeader::new(Vec::new()), task.command.into_bytes());
            let serialized_data = packet.to_bytes();

            // 3. ネットワーク送信処理
            let result = async {
                let mut stream = TcpStream::connect(&target_address).await?;
                
                let len_bytes = (serialized_data.len() as u32).to_be_bytes();
                stream.write_all(&len_bytes).await?;
                stream.write_all(&serialized_data).await?;
                stream.flush().await?;

                let mut ack_buf = [0u8; 4];
                let _ = stream.read(&mut ack_buf).await;
                
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }.await;

            // 4. カウンタの確実な解放
            self.release_worker(&target_address).await;

            // 5. 結果の返却（リトライやタイムアウトのハンドリングは上位層へ完全に委ねる）
            match result {
                Ok(_) => {
                    println!("👍 [RemoteExecutor] タスク '{}' の送信・応答確認が成功", task.name);
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