// src/core/retry.rs

use crate::core::graph::TaskResult;

/// リトライポリシーを定義する構造体
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base_delay_ms: u64,
}

impl RetryPolicy {
    pub fn new(max_retries: u32, base_delay_ms: u64) -> Self {
        Self {
            max_retries,
            base_delay_ms,
        }
    }

    /// インフラ障害（InfraError）の場合のみリトライ対象とする判定ロジック
    pub fn should_retry(&self, attempt: u32, result: &TaskResult) -> bool {
        if attempt >= self.max_retries {
            return false;
        }

        matches!(result, TaskResult::InfraError { .. })
    }
}