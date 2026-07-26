use crate::protocol::{TaskResultSpec, TaskSpec};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::timeout;
use tracing::{error, info, instrument};

pub struct TaskExecutor;

impl TaskExecutor {
    pub fn new() -> Self {
        Self
    }

    /// タスクを実行し、`TaskResultSpec` を返す
    #[instrument(skip(self, task), fields(task_id = %task.task_id))]
    pub async fn execute(&self, task: &TaskSpec) -> TaskResultSpec {
        let task_id = task.task_id.clone();
        info!("Executing task: {}", task_id);

        let mut cmd = Command::new(&task.command);
        cmd.args(&task.args);
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        match cmd.output().await {
            Ok(output) => {
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                let success = output.status.success();
                let exit_code = output.status.code();

                if success {
                    info!("Task {} completed successfully", task_id);
                } else {
                    error!("Task {} failed with exit code: {:?}", task_id, exit_code);
                }

                TaskResultSpec {
                    task_id,
                    success,
                    exit_code,
                    stdout,
                    stderr,
                }
            }
            Err(e) => {
                error!("Failed to execute command for task {}: {}", task_id, e);
                TaskResultSpec {
                    task_id,
                    success: false,
                    exit_code: None,
                    stdout: String::new(),
                    stderr: format!("Failed to spawn process: {}", e),
                }
            }
        }
    }

    /// タイムアウト付きでタスクを実行する
    #[instrument(skip(self, task), fields(task_id = %task.task_id, timeout_secs = timeout_secs))]
    pub async fn execute_with_timeout(&self, task: &TaskSpec, timeout_secs: u64) -> TaskResultSpec {
        let duration = Duration::from_secs(timeout_secs);

        match timeout(duration, self.execute(task)).await {
            Ok(result) => result,
            Err(_) => {
                error!("Task {} timed out after {} seconds", task.task_id, timeout_secs);
                TaskResultSpec {
                    task_id: task.task_id.clone(),
                    success: false,
                    exit_code: None,
                    stdout: String::new(),
                    stderr: format!("Task execution timed out after {} seconds", timeout_secs),
                }
            }
        }
    }
}

impl Default for TaskExecutor {
    fn default() -> Self {
        Self::new()
    }
}