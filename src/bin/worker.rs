// src/bin/worker.rs

use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::Command as AsyncCommand;
use tokio::time::timeout;
use tracing::{error, info, warn};

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
    pub async fn execute(&self, command_str: &str) -> ExecutionResult {
        let start_time = Instant::now();

        // OSに応じたコマンドの構築
        let mut cmd = if cfg!(target_os = "windows") {
            let mut c = AsyncCommand::new("cmd");
            c.args(["/C", command_str]);
            c.kill_on_drop(true); // タイムアウト時に裏で残る子プロセスを即座にKill
            c
        } else {
            let mut c = AsyncCommand::new("sh");
            c.arg("-c").arg(command_str);
            c.kill_on_drop(true); // UNIX系でもタイムアウト時にプロセスをクリーンアップ
            c
        };

        info!("🚀 [Executor] タスク実行開始: '{}'", command_str);

        // タイムアウト付きで非同期プロセスを実行
        let result = timeout(self.default_timeout, cmd.output()).await;

        let elapsed = start_time.elapsed().as_millis() as u64;

        match result {
            Ok(Ok(output)) => {
                if output.status.success() {
                    info!("✅ [Executor] タスク成功 (実行時間: {}ms)", elapsed);
                    ExecutionResult {
                        status: "SUCCESS".to_string(),
                        execution_time_ms: elapsed,
                    }
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    warn!("⚠️ [Executor] タスクエラー終了: {}", stderr.trim());
                    ExecutionResult {
                        status: "FAILED".to_string(),
                        execution_time_ms: elapsed,
                    }
                }
            }
            Ok(Err(e)) => {
                error!("❌ [Executor] プロセス起動失敗: {:?}", e);
                ExecutionResult {
                    status: "FAILED".to_string(),
                    execution_time_ms: elapsed,
                }
            }
            Err(_) => {
                warn!("⏰ [Executor] タスクがタイムアウトしました (上限: {:?})", self.default_timeout);
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
    // tracingの初期化
    tracing_subscriber::fmt::init();

    info!("=== 🥷 Ninja Distributed Worker (Client Mode) ===");
    info!("💡 終了するには 'q' を入力して Enter を押すか、Ctrl + C を押してください。");

    let server_addr = "127.0.0.1:9001";
    info!("📡 [Worker] マスターサーバー ( {} ) に接続しています...", server_addr);

    let stream = TcpStream::connect(server_addr).await?;
    info!("✓ [Worker] マスターに正常に接続しました。指示を待機します。");

    // TcpStream を Split してソケット入出力を別々に扱う
    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);

    // デフォルトタイムアウト10秒でExecutorを初期化
    let executor = TaskExecutor::new(Duration::from_secs(10));

    let mut stdin = tokio::io::stdin();
    let mut key_buf = [0u8; 1];

    loop {
        let mut line = String::new();

        tokio::select! {
            // 'q' または 'Q' 監視
            res = stdin.read(&mut key_buf) => {
                if let Ok(n) = res {
                    if n > 0 && (key_buf[0] == b'q' || key_buf[0] == b'Q') {
                        info!("🛑 [Worker] 'q' キーを検知しました。Workerを終了します...");
                        break;
                    }
                }
            }
            // Masterからの命令待機 (行単位で受領)
            res = buf_reader.read_line(&mut line) => {
                match res {
                    Ok(0) => {
                        info!("👋 [Worker] マスターとの接続が切断されました。");
                        break;
                    }
                    Ok(_) => {
                        let command_str = line.trim().to_string();
                        if command_str.is_empty() {
                            continue;
                        }

                        info!("📥 [Worker] マスターからタスクを受信しました: {:?}", command_str);

                        // Executorでタスクを実行
                        let exec_result = executor.execute(&command_str).await;

                        // 結果の報告（Masterが read_line できるよう末尾に \n を付与して flush）
                        let response_payload = format!("{}\n", exec_result.status);
                        if let Err(e) = writer.write_all(response_payload.as_bytes()).await {
                            error!("❌ [Worker] レスポンス送信失敗: {:?}", e);
                            break;
                        }
                        if let Err(e) = writer.flush().await {
                            error!("❌ [Worker] Flush失敗: {:?}", e);
                            break;
                        }

                        info!("📤 [Worker] 実行結果 [{}] ({}ms) をマスターに報告しました。\n",
                            exec_result.status, exec_result.execution_time_ms);
                    }
                    Err(e) => {
                        error!("❌ [Worker] 通信エラー: {:?}", e);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}