#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::core::graph::Task;
use crate::core::path::PathHeader;
use crate::core::packet::NinjaPacket;
use crate::core::worker::WorkerRegistry;

/// 🥈 🟡 TaskResult: タスクの終了コンテキストを詳細に保持する構造化列挙型
#[derive(Debug, Clone)]
pub enum TaskResult {
    /// 正常終了（ワーカーからの戻り値メッセージを含む）
    Success(String),
    /// ワーカー側の処理・コマンド実行そのものの失敗
    TaskFailed { reason: String },
    /// 応答が時間内に返ってこなかった
    Timeout,
    /// ネットワーク切断や接続拒否など、システム・通信インフラ起因のエラー（node: 発生元アドレス）
    InfraError { node: String, reason: String },
}

pub type ExecutorResult = Result<TaskResult, Box<dyn std::error::Error + Send + Sync>>;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// ----------------------------------------------------
/// 実行と結果判定を担当する Executor トレイト
/// ----------------------------------------------------
pub trait Executor: Send + Sync {
    /// 指定されたターゲット（ワーカー）へタスクを送信し、詳細な実行結果（TaskResult）を返す
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
            let node_addr = target_address.clone();

            // 1. レジストリを通じて安全にカウンタをインクリメント
            if let Err(e) = self.registry.acquire(&target_address).await {
                return Ok(TaskResult::InfraError {
                    node: node_addr,
                    reason: format!("レジストリでのアクワイアに失敗: {}", e),
                });
            }

            println!("📤 [RemoteExecutor] タスク '{}' をワーカー '{}' へ純粋送信中...", task.name, target_address);

            // 2. パケットの構築とシリアライズ
            let packet = NinjaPacket::new(64, PathHeader::new(Vec::new()), task.command.into_bytes());
            let serialized_data = packet.to_bytes();

            // 3. ネットワーク送信および応答パース処理
            let network_result: Result<TaskResult, Box<dyn std::error::Error + Send + Sync>> = async {
                let mut stream = TcpStream::connect(&target_address).await?;
                
                let len_bytes = (serialized_data.len() as u32).to_be_bytes();
                stream.write_all(&len_bytes).await?;
                stream.write_all(&serialized_data).await?;
                stream.flush().await?;

                let mut ack_buf = [0u8; 4];
                let _ = stream.read(&mut ack_buf).await;
                
                let response_str = String::from_utf8_lossy(&ack_buf).trim().to_string();
                if response_str.starts_with("OK") {
                    Ok(TaskResult::Success(response_str))
                } else {
                    Ok(TaskResult::TaskFailed {
                        reason: format!("ワーカー側で不正な応答を検出: {}", response_str),
                    })
                }
            }.await;

            // 4. レジストリを通じて確実に解放
            self.registry.release(&target_address).await;

            // 5. 結果の判定とラップ
            match network_result {
                Ok(task_res) => {
                    match &task_res {
                        TaskResult::Success(_) => println!("👍 [RemoteExecutor] タスク '{}' が正常終了", task.name),
                        TaskResult::TaskFailed { reason } => println!("⚠️ [RemoteExecutor] タスク '{}' がワーカー側で失敗: {}", task.name, reason),
                        _ => {}
                    }
                    Ok(task_res)
                }
                Err(e) => {
                    eprintln!("💥 [RemoteExecutor] 通信インフラレベルのエラーを検出 [タスク: {}]: {}", task.name, e);
                    Ok(TaskResult::InfraError {
                        node: node_addr,
                        reason: e.to_string(),
                    })
                }
            }
        })
    }
}