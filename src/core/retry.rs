#![allow(dead_code)]

use std::time::Duration;
use crate::core::executor::TaskResult;

/// 🥇 🟡 RetryPolicy トレイトの定義
pub trait RetryPolicy: Send + Sync {
    /// 実行結果と現在の累積リトライ回数から、さらにリトライすべきかを判定する
    fn should_retry(&self, result: &TaskResult, current_retries: usize, max_retries: usize) -> bool;

    /// 試行回数に応じたバックオフ（待機）時間を返す
    fn backoff(&self, attempt: usize) -> Duration;
}

/// 🚀 エクスポネンシャルバックオフを備えたデフォルトのリトライ戦略
pub struct DefaultRetryPolicy {
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl DefaultRetryPolicy {
    pub fn new(base_delay: Duration, max_delay: Duration) -> Self {
        Self { base_delay, max_delay }
    }
}

impl Default for DefaultRetryPolicy {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(3),
        }
    }
}

impl RetryPolicy for DefaultRetryPolicy {
    fn should_retry(&self, result: &TaskResult, current_retries: usize, max_retries: usize) -> bool {
        match result {
            TaskResult::Success(_) => false,
            // インフラエラー時は迂回を期待して大目に回す
            TaskResult::InfraError { .. } => current_retries < max_retries * 2,
            // タスク単体のエラーやタイムアウトは通常の回数制限
            TaskResult::TaskFailed { .. } | TaskResult::Timeout => current_retries < max_retries,
        }
    }

    fn backoff(&self, attempt: usize) -> Duration {
        if attempt == 0 {
            return Duration::from_millis(10);
        }
        let exponent = 2u32.saturating_pow(attempt.min(10) as u32);
        let delay = self.base_delay * exponent;
        delay.min(self.max_delay)
    }
}