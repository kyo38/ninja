// src/core/executor.rs

use async_trait::async_trait;
pub use crate::core::graph::TaskResult;
use crate::core::graph::Task;

#[async_trait]
pub trait Executor: Send + Sync {
    async fn submit(&self, task: Task, worker_address: String) -> Result<TaskResult, String>;
}

pub struct RemoteExecutor;

impl RemoteExecutor {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Executor for RemoteExecutor {
    async fn submit(&self, task: Task, worker_address: String) -> Result<TaskResult, String> {
        // フェーズ2の通信・リトライ処理用領域
        Ok(TaskResult::Success {
            stdout: format!("Executed {} on {}", task.name, worker_address),
        })
    }
}

pub struct MockExecutor;

#[async_trait]
impl Executor for MockExecutor {
    async fn submit(&self, task: Task, _worker_address: String) -> Result<TaskResult, String> {
        Ok(TaskResult::Success {
            stdout: format!("Mock executed {}", task.name),
        })
    }
}