#![allow(dead_code)]

use async_trait::async_trait;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskResult {
    Success {
        worker: String,
        duration: std::time::Duration,
        attempt: usize,
        message: String,
    },
    TaskFailed {
        worker: String,
        duration: std::time::Duration,
        attempt: usize,
        reason: String,
    },
    Timeout {
        worker: String,
        duration: std::time::Duration,
    },
    InfraError {
        node: String,
        reason: String,
    },
}

#[async_trait]
pub trait Executor: Send + Sync {
    async fn submit(&self, task: crate::core::graph::Task, worker_address: String) -> Result<TaskResult, String>;
}

/// メイン処理から呼び出されるリモートエグゼキュータの実体
pub struct RemoteExecutor;

impl RemoteExecutor {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Executor for RemoteExecutor {
    async fn submit(&self, task: crate::core::graph::Task, worker_address: String) -> Result<TaskResult, String> {
        // ダミーの通信・実行シミュレーション
        Ok(TaskResult::Success {
            worker: worker_address,
            duration: std::time::Duration::from_millis(100),
            attempt: 1,
            message: format!("Command '{}' executed successfully via RemoteExecutor", task.command),
        })
    }
}

/// テスト用のMockエグゼキュータ
pub struct MockExecutor;

#[async_trait]
impl Executor for MockExecutor {
    async fn submit(&self, task: crate::core::graph::Task, worker_address: String) -> Result<TaskResult, String> {
        Ok(TaskResult::Success {
            worker: worker_address,
            duration: std::time::Duration::from_millis(10),
            attempt: 1,
            message: format!("Mock success for {}", task.name),
        })
    }
}