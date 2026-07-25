// src/bin/worker.rs

use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::Command as AsyncCommand;
use tokio::time::{interval, sleep, timeout, MissedTickBehavior};
use tracing::{debug, error, info, instrument, warn};
use tracing_subscriber::{fmt, EnvFilter};

/// タスク実行結果を表す構造体
#[derive(Debug)]
pub struct ExecutionResult {
    pub status: String,
    pub execution_time_ms: u64,
}

/// タスクの実行を担当する Executor
pub struct TaskExecutor {
    pub default_timeout: Duration,
}

impl TaskExecutor {
    pub fn new(default_timeout: Duration) -> Self {
        Self { default_timeout }
    }

    /// コマンドをタイムアウト付きで非同期実行する
    #[instrument(skip(self), fields(command = %command_str))]
    pub async fn execute(&self, command_str: &str) -> ExecutionResult {
        let start_time = Instant::now();

        // OSに応じたコマンドの構築
        let mut cmd = if cfg!(target_os = "windows") {
            let mut c = AsyncCommand::new("cmd");
            c.args(["/C", command_str]);
            c.kill_on_drop(true);
            c
        } else {
            let mut c = AsyncCommand::new("sh");
            c.arg("-c").arg(command_str);
            c.kill_on_drop(true);
            c
        };

        info!("🚀 タスク実行を開始します");

        let result = timeout(self.default_timeout, cmd.output()).await;
        let elapsed = start_time.elapsed().as_millis() as u64;

        match result {
            Ok(Ok(output)) => {
                if output.status.success() {
                    info!(elapsed_ms = elapsed, "✅ タスクが正常終了しました");
                    ExecutionResult {
                        status: "SUCCESS".to_string(),
                        execution_time_ms: elapsed,
                    }
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    warn!(
                        elapsed_ms = elapsed,
                        stderr = %stderr.trim(),
                        "⚠️ タスクがエラー終了しました"
                    );
                    ExecutionResult {
                        status: "FAILED".to_string(),
                        execution_time_ms: elapsed,
                    }
                }
            }
            Ok(Err(e)) => {
                error!(error = %e, elapsed_ms = elapsed, "❌ プロセス起動に失敗しました");
                ExecutionResult {
                    status: "FAILED".to_string(),
                    execution_time_ms: elapsed,
                }
            }
            Err(_) => {
                warn!(
                    timeout_secs = self.default_timeout.as_secs(),
                    elapsed_ms = elapsed,
                    "⏰ タスクがタイムアウトしました"
                );
                ExecutionResult {
                    status: "TIMED_OUT".to_string(),
                    execution_time_ms: elapsed,
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(true)
        .init();

    info!("=== 🥷 Ninja Distributed Worker (Client Mode) ===");
    info!("💡 終了するには 'q' を入力して Enter を押すか、Ctrl + C を押してください。");

    let server_addr = "127.0.0.1:9001";
    let executor = TaskExecutor::new(Duration::from_secs(10));
    let mut reconnect_delay = Duration::from_secs(1);
    let max_reconnect_delay = Duration::from_secs(30);

    let mut stdin = tokio::io::stdin();
    let mut key_buf = [0u8; 1];

    // 外側の再接続ループ
    'reconnect: loop {
        info!(server_addr = %server_addr, "📡 マスターサーバーに接続しています...");

        let stream = match TcpStream::connect(server_addr).await {
            Ok(stream) => {
                info!("✓ マスターに正常に接続しました。指示を待機します。");
                reconnect_delay = Duration::from_secs(1);
                stream
            }
            Err(e) => {
                warn!(
                    error = %e,
                    retry_in_secs = reconnect_delay.as_secs(),
                    "⚠️ マスターへの接続に失敗しました。再試行します..."
                );

                tokio::select! {
                    res = stdin.read(&mut key_buf) => {
                        if let Ok(n) = res {
                            if n > 0 && (key_buf[0] == b'q' || key_buf[0] == b'Q') {
                                info!("🛑 'q' キーを検知しました。Workerを終了します...");
                                break 'reconnect;
                            }
                        }
                    }
                    _ = sleep(reconnect_delay) => {}
                }

                reconnect_delay = std::cmp::min(reconnect_delay * 2, max_reconnect_delay);
                continue 'reconnect;
            }
        };

        let (reader, mut writer) = stream.into_split();
        let mut buf_reader = BufReader::new(reader);

        // ハートビート用のタイマー設定（5秒周期）
        let mut heartbeat_interval = interval(Duration::from_secs(5));
        // 遅延発生時にスキップして追いつく挙動を設定
        heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // 通信セッションループ
        loop {
            let mut line = String::new();

            tokio::select! {
                // 1. 'q' キー監視
                res = stdin.read(&mut key_buf) => {
                    if let Ok(n) = res {
                        if n > 0 && (key_buf[0] == b'q' || key_buf[0] == b'Q') {
                            info!("🛑 'q' キーを検知しました。Workerを終了します...");
                            break 'reconnect;
                        }
                    }
                }

                // 2. 定期ハートビート (PING 送信)
                _ = heartbeat_interval.tick() => {
                    debug!("💓 [Heartbeat] PING を送信します");
                    if let Err(e) = writer.write_all(b"PING\n").await {
                        error!(error = %e, "❌ PING 送信失敗。再接続します...");
                        break;
                    }
                    if let Err(e) = writer.flush().await {
                        error!(error = %e, "❌ PING Flush失敗。再接続します...");
                        break;
                    }
                }

                // 3. Master からの受信処理
                res = buf_reader.read_line(&mut line) => {
                    match res {
                        Ok(0) => {
                            warn!("👋 マスターとの接続が切断されました。再接続を試みます...");
                            break;
                        }
                        Ok(_) => {
                            let msg = line.trim().to_string();
                            if msg.is_empty() {
                                continue;
                            }

                            // PONG 応答の受領ハンドリング
                            if msg == "PONG" {
                                debug!("💓 [Heartbeat] PONG を受領しました");
                                continue;
                            }

                            info!(command = %msg, "📥 マスターからタスクを受信しました");

                            let exec_result = executor.execute(&msg).await;

                            let response_payload = format!("{}\n", exec_result.status);
                            if let Err(e) = writer.write_all(response_payload.as_bytes()).await {
                                error!(error = %e, "❌ レスポンス送信失敗。再接続します...");
                                break;
                            }
                            if let Err(e) = writer.flush().await {
                                error!(error = %e, "❌ Flush失敗。再接続します...");
                                break;
                            }

                            info!(
                                status = %exec_result.status,
                                elapsed_ms = exec_result.execution_time_ms,
                                "📤 実行結果をマスターに報告しました"
                            );
                        }
                        Err(e) => {
                            error!(error = %e, "❌ 通信エラーが発生しました。再接続します...");
                            break;
                        }
                    }
                }
            }
        }

        sleep(reconnect_delay).await;
    }

    Ok(())
}