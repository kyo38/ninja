// src/core/retry.rs
use std::future::Future;
use std::time::Duration;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(10),
            backoff_factor: 2.0,
        }
    }
}

impl RetryConfig {
    pub fn new(max_retries: usize, initial_delay_ms: u64) -> Self {
        Self {
            max_retries,
            initial_delay: Duration::from_millis(initial_delay_ms),
            ..Default::default()
        }
    }

    /// 指数バックオフ対応の非同期リトライ実行関数
    pub async fn execute<F, Fut, T, E>(&self, task_id: &str, mut operation: F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let mut attempts = 0;
        let mut current_delay = self.initial_delay;

        loop {
            attempts += 1;
            match operation().await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if attempts > self.max_retries {
                        warn!(
                            "[Task:{}] 制限リトライ回数({})に達したため失敗と判定します: {}",
                            task_id, self.max_retries, err
                        );
                        return Err(err);
                    }

                    warn!(
                        "[Task:{}] 実行失敗 (試行 {}/{}): {}. {:?} 後に再試行します...",
                        task_id, attempts, self.max_retries, err, current_delay
                    );

                    tokio::time::sleep(current_delay).await;

                    // 指数バックオフ計算
                    let next_delay_secs = current_delay.as_secs_f64() * self.backoff_factor;
                    current_delay = Duration::from_secs_f64(next_delay_secs).min(self.max_delay);
                }
            }
        }
    }
}