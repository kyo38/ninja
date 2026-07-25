// src/server/orchestrator.rs

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::core::config::Config;
use crate::error::Result;

static NEXT_WORKER_ID: AtomicU64 = AtomicU64::new(1);

pub struct Orchestrator {
    config: Config,
}

impl Orchestrator {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn from_config(config: Config) -> Self {
        Self::new(config)
    }

    pub async fn run(&self) -> Result<()> {
        info!("💡 終了するには 'q' を入力して Enter を押すか、Ctrl + C を押してください。");

        let worker_listener = TcpListener::bind(&self.config.worker_addr).await?;
        info!("📡 [Master] Workerからの接続を待機中... (ポート: {})", self.config.worker_addr);

        let client_listener = TcpListener::bind(&self.config.client_addr).await?;
        info!("📡 [Master] クライアントからのDAGタスク投入を待機中... (ポート: {})", self.config.client_addr);

        let mut stdin = tokio::io::stdin();
        let mut key_buf = [0u8; 1];

        loop {
            tokio::select! {
                // 'q' キーでの終了判定
                res = stdin.read(&mut key_buf) => {
                    if let Ok(n) = res {
                        if n > 0 && (key_buf[0] == b'q' || key_buf[0] == b'Q') {
                            info!("🛑 'q' キーを検知しました。Masterを終了します...");
                            break;
                        }
                    }
                }

                // Worker からの接続受入
                accept_res = worker_listener.accept() => {
                    match accept_res {
                        Ok((stream, peer_addr)) => {
                            tokio::spawn(Self::handle_worker(stream, peer_addr));
                        }
                        Err(e) => {
                            error!(error = %e, "❌ Worker接続アクセプトエラー");
                        }
                    }
                }

                // クライアントからの接続受入（ダミー受領）
                client_res = client_listener.accept() => {
                    if let Ok((_stream, peer_addr)) = client_res {
                        info!(addr = %peer_addr, "📩 クライアントからのDAGタスク要求を受け取りました");
                    }
                }
            }
        }

        Ok(())
    }

    /// Worker 1台との通信・生存確認（ハートビート）を担当するループ
    async fn handle_worker(stream: TcpStream, peer_addr: std::net::SocketAddr) {
        let worker_id = NEXT_WORKER_ID.fetch_add(1, Ordering::SeqCst);
        info!(
            worker_id = worker_id,
            addr = %peer_addr,
            "🤝 [Master] Workerがクラスタに参加しました"
        );

        let (reader, mut writer) = stream.into_split();
        let mut buf_reader = BufReader::new(reader);

        // 15秒間何も通信がなければノード離脱（死んだ）と判断
        let heartbeat_timeout = Duration::from_secs(15);

        loop {
            let mut line = String::new();

            match timeout(heartbeat_timeout, buf_reader.read_line(&mut line)).await {
                Ok(Ok(0)) => {
                    info!(
                        worker_id = worker_id,
                        "👋 Workerとの接続が切断されました（正常切断）"
                    );
                    break;
                }
                Ok(Ok(_)) => {
                    let msg = line.trim().to_string();
                    if msg.is_empty() {
                        continue;
                    }

                    if msg == "PING" {
                        debug!(worker_id = worker_id, "💓 [Heartbeat] PINGを受信 -> PONGを返信します");
                        if let Err(e) = writer.write_all(b"PONG\n").await {
                            error!(
                                worker_id = worker_id,
                                error = %e,
                                "❌ PONG送信エラー。接続を破棄します"
                            );
                            break;
                        }
                        if let Err(e) = writer.flush().await {
                            error!(
                                worker_id = worker_id,
                                error = %e,
                                "❌ PONG Flushエラー。接続を破棄します"
                            );
                            break;
                        }
                    } else {
                        info!(
                            worker_id = worker_id,
                            response = %msg,
                            "📥 Workerからタスク実行結果を受信しました"
                        );
                    }
                }
                Ok(Err(e)) => {
                    error!(
                        worker_id = worker_id,
                        error = %e,
                        "❌ Workerとの通信中にエラーが発生しました"
                    );
                    break;
                }
                Err(_) => {
                    warn!(
                        worker_id = worker_id,
                        timeout_secs = heartbeat_timeout.as_secs(),
                        "⏰ [Heartbeat] タイムアウト！Workerからの応答が絶たれたため接続を解放します"
                    );
                    break;
                }
            }
        }

        info!(worker_id = worker_id, "🧹 Workerの管理リソースをクリーンアップしました");
    }
}