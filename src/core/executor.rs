// src/core/executor.rs
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tracing::{info, warn, instrument};
use crate::core::error::NinjaError;
use crate::core::graph::{Task, TaskResult};
use crate::core::retry::RetryConfig;

pub struct Executor {
    retry_config: RetryConfig,
    semaphore: Arc<Semaphore>,
}

impl Default for Executor {
    fn default() -> Self {
        Self {
            retry_config: RetryConfig::default(),
            semaphore: Arc::new(Semaphore::new(4)),
        }
    }
}

impl Executor {
    pub fn new(retry_config: RetryConfig, max_concurrency: usize) -> Self {
        Self {
            retry_config,
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
        }
    }

    /// リトライ機構・タイムアウト制御・並列制御を備えたタスク実行インターフェース
    #[instrument(skip(self, task), fields(task_name = %task.name))]
    pub async fn execute(&self, task: Task) -> TaskResult {
        let task_name = task.name.clone();
        
        // 並列実行数の制御（セマフォからPermitを取得）
        let _permit = match self.semaphore.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                warn!("セマフォの取得に失敗しました (Executorが終了状態の可能性があります)");
                return TaskResult::Failure {
                    exit_code: 1,
                    stderr: "Semaphore acquired failed (executor closed)".to_string(),
                };
            }
        };

        info!("タスク実行を開始します");

        let retry_config = self.retry_config.clone();
        let task_ref = Arc::new(task);

        let result = retry_config
            .execute(&task_name, || {
                let t = Arc::clone(&task_ref);
                async move { self.run_task_with_timeout(&t).await }
            })
            .await;

        match result {
            Ok(output) => {
                info!("タスクが正常終了しました");
                TaskResult::Success { stdout: output }
            }
            Err(err) => {
                warn!(error = %err, "タスクが最終的に失敗しました");
                TaskResult::Failure {
                    exit_code: 1,
                    stderr: err.to_string(),
                }
            }
        }
    }

    /// タイムアウトを考慮した単一タスクの実行ラッパー
    #[instrument(skip(self, task), fields(task_name = %task.name, timeout_secs = task.timeout_secs))]
    async fn run_task_with_timeout(&self, task: &Task) -> Result<String, NinjaError> {
        let timeout_duration = Duration::from_secs(task.timeout_secs);

        match timeout(timeout_duration, self.run_task_internal(task)).await {
            Ok(execution_result) => execution_result,
            Err(_) => {
                warn!("タスク実行がタイムアウトしました");
                Err(NinjaError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Task execution timed out after {} seconds", task.timeout_secs),
                )))
            }
        }
    }

    /// 単一タスクの生実行ロジック
    #[instrument(skip(self, task), fields(command = %task.command))]
    async fn run_task_internal(&self, task: &Task) -> Result<String, NinjaError> {
        // コマンド実行のシミュレーション
        if task.command.contains("fail") {
            return Err(NinjaError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Simulated task failure for retry testing",
            )));
        }

        // タイムアウト検証用の擬似遅延コマンド（例: "sleep_5"）
        if task.command.contains("sleep_5") {
            tokio::time::sleep(Duration::from_secs(5)).await;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok(format!("Completed: {}", task.command))
    }
}