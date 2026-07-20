#![allow(dead_code)]

use crate::core::executor::TaskResult;

pub trait RetryPolicy: Send + Sync {
    fn should_retry(&self, result: &TaskResult, current_retries: usize, max_retries: usize) -> bool;
    fn backoff(&self, failure_count: usize) -> std::time::Duration;
}

pub struct FixedIntervalRetry {
    pub interval: std::time::Duration,
}

impl FixedIntervalRetry {
    pub fn new(interval_ms: u64) -> Self {
        Self {
            interval: std::time::Duration::from_millis(interval_ms),
        }
    }
}

// main.rs での DefaultRetryPolicy::default() 呼び出しに対応
impl Default for FixedIntervalRetry {
    fn default() -> Self {
        Self {
            interval: std::time::Duration::from_millis(1000),
        }
    }
}

impl RetryPolicy for FixedIntervalRetry {
    fn should_retry(&self, result: &TaskResult, current_retries: usize, max_retries: usize) -> bool {
        match result {
            TaskResult::Success { .. } => false,
            TaskResult::InfraError { .. } => true,
            TaskResult::TaskFailed { .. } | TaskResult::Timeout { .. } => current_retries < max_retries,
        }
    }

    fn backoff(&self, _failure_count: usize) -> std::time::Duration {
        self.interval
    }
}

// 外部から DefaultRetryPolicy 名義で呼び出せるようにエイリアスを定義
pub type DefaultRetryPolicy = FixedIntervalRetry;