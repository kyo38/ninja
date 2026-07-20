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

// =========================================================================
// 🧪 ユニットテスト領域
// =========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_should_not_retry_on_success() {
        let policy = FixedIntervalRetry::new(100);
        let success_result = TaskResult::Success {
            worker: "127.0.0.1:8080".to_string(),
            duration: Duration::from_millis(50),
            attempt: 1,
            message: "Success".to_string(),
        };

        // 成功時はリトライカウントや上限に関わらず常に false
        assert!(!policy.should_retry(&success_result, 0, 3));
        assert!(!policy.should_retry(&success_result, 3, 3));
    }

    #[test]
    fn test_should_always_retry_on_infra_error() {
        let policy = FixedIntervalRetry::new(100);
        let infra_error = TaskResult::InfraError {
            node: "127.0.0.1:8080".to_string(),
            reason: "Connection refused".to_string(),
        };

        // インフラエラーは上限に達していても、別のルートでの回復を期待して常に true
        assert!(policy.should_retry(&infra_error, 0, 3));
        assert!(policy.should_retry(&infra_error, 3, 3));
        assert!(policy.should_retry(&infra_error, 5, 3));
    }

    #[test]
    fn test_should_retry_on_task_failed_until_max() {
        let policy = FixedIntervalRetry::new(100);
        let failed_result = TaskResult::TaskFailed {
            worker: "127.0.0.1:8080".to_string(),
            duration: Duration::from_millis(120),
            attempt: 1,
            reason: "Exit code 1".to_string(),
        };

        // 上限未満であれば true
        assert!(policy.should_retry(&failed_result, 0, 3));
        assert!(policy.should_retry(&failed_result, 2, 3));
        
        // 上限に達した、あるいは超えたら false
        assert!(!policy.should_retry(&failed_result, 3, 3));
        assert!(!policy.should_retry(&failed_result, 4, 3));
    }

    #[test]
    fn test_should_retry_on_timeout_until_max() {
        let policy = FixedIntervalRetry::new(100);
        let timeout_result = TaskResult::Timeout {
            worker: "127.0.0.1:8080".to_string(),
            duration: Duration::from_secs(30),
        };

        // タイムアウトも通常のタスク失敗と同様、上限未満であれば true
        assert!(policy.should_retry(&timeout_result, 0, 3));
        assert!(policy.should_retry(&timeout_result, 2, 3));
        
        // 上限到達・超過で false
        assert!(!policy.should_retry(&timeout_result, 3, 3));
    }

    #[test]
    fn test_backoff_returns_fixed_interval() {
        let policy = FixedIntervalRetry::new(500);
        
        // 失敗回数に関わらず一律で設定されたインターバル（500ms）を返すか検証
        assert_eq!(policy.backoff(1), Duration::from_millis(500));
        assert_eq!(policy.backoff(3), Duration::from_millis(500));
    }
}