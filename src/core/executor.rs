#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::core::graph::Task;
use crate::core::path::PathHeader;
use crate::core::packet::NinjaPacket;
use crate::core::worker::WorkerRegistry; // ✨ WorkerRegistryをインポート

pub type ExecutorResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// ----------------------------------------------------
/// 実行だけを担当するシンプルな Executor トレイト
/// ----------------------------------------------------
pub trait Executor: Send + Sync {
    /// 指定されたターゲット（ワーカー）へタスクを送信し、実行を委ねる
    fn submit<'a>(
        &'a self,
        task: Task,
        target_address: String,
    ) -> BoxFuture<'a, ExecutorResult>;
}

/// ----------------------------------------------------
/// 【Data Plane】純粋な通信実行に特化した RemoteExecutor
/// ----------------------------------------------------
pub struct RemoteExecutor {
    // ✨ 内部に生の Arc<Mutex<Vec<...>>> を持たず、レジストリを受け取る
    pub registry: WorkerRegistry,
}

impl RemoteExecutor {
    pub fn new(registry: WorkerRegistry) -> Self {
        Self { registry }
    }
}

/// ----------------------------------------------------
/// RemoteExecutor に対する Executor トレイトの実装
/// ----------------------------------------------------
impl Executor for RemoteExecutor {
    fn submit<'a>(
        &'a self,
        task: Task,
        target_address: String,
    ) -> BoxFuture<'a, ExecutorResult> {
        Box::pin(async move {
            // 1. レジストリを通じて安全にカウンタをインクリメント
            if let Err(e) = self.registry.acquire(&target_address).await {
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

            // 4. レジストリを通じて確実に解放
            self.registry.release(&target_address).await;

            // 5. 結果の返却
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