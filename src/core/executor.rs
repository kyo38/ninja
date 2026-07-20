use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{self, Duration, Instant};
use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt};

use crate::core::path::{PathHeader, HopField, PathStrategy};
use crate::core::packet::NinjaPacket;

/// ----------------------------------------------------
/// 【Data Plane】ワーカーの生存状態とメトリクスを管理
/// ----------------------------------------------------
#[derive(Debug, Clone)]
pub struct WorkerStatus {
    pub address: String,
    pub is_alive: bool,
    pub is_busy: bool,
    pub latency_ms: u16,
}

/// ----------------------------------------------------
/// 【Data Plane】実際の通信と実行を担うエグゼキュータ
/// ----------------------------------------------------
#[derive(Debug)]
pub struct RemoteExecutor {
    pub workers: Arc<Mutex<Vec<WorkerStatus>>>,
}

impl RemoteExecutor {
    pub fn new(addresses: Vec<String>) -> Self {
        let workers = addresses
            .into_iter()
            .map(|addr| WorkerStatus {
                address: addr,
                is_alive: true,
                is_busy: false,
                latency_ms: 999,
            })
            .collect();

        Self {
            workers: Arc::new(Mutex::new(workers)),
        }
    }

    /// バックグラウンドで定期的にヘルスチェックとレイテンシ計測を行う
    pub async fn start_heartbeat_loop(self: &Arc<Self>, interval: Duration, timeout: Duration) {
        let workers_clone = Arc::clone(&self.workers);

        tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            loop {
                ticker.tick().await;
                let mut workers = workers_clone.lock().await;

                for worker in workers.iter_mut() {
                    let start = Instant::now();
                    let check_fut = TcpStream::connect(&worker.address);
                    
                    match time::timeout(timeout, check_fut).await {
                        Ok(Ok(_stream)) => {
                            let rtt = start.elapsed().as_millis() as u16;
                            worker.latency_ms = rtt;

                            if !worker.is_alive {
                                println!("💖 [Heartbeat] ワーカー ( {} ) 復帰 [RTT: {}ms]", worker.address, rtt);
                                worker.is_alive = true;
                            }
                        }
                        _ => {
                            if worker.is_alive {
                                println!("🔴 [Heartbeat] ワーカー ( {} ) のダウンを検知", worker.address);
                                worker.is_alive = false;
                                worker.is_busy = false;
                                worker.latency_ms = 999;
                            }
                        }
                    }
                }
            }
        });
    }

    /// 【SCION構造のコア】戦略に基づいて最適なPathを生成・選択する
    pub async fn select_path(&self, strategy: PathStrategy) -> Option<(String, PathHeader)> {
        let mut workers = self.workers.lock().await;
        let mut candidate_idx: Option<usize> = None;

        match strategy {
            PathStrategy::Shortest | PathStrategy::Available => {
                if let Some(pos) = workers.iter().position(|w| w.is_alive && !w.is_busy) {
                    candidate_idx = Some(pos);
                }
            }
            PathStrategy::Fastest => {
                let mut min_latency = u16::MAX;
                for (i, worker) in workers.iter().enumerate() {
                    if worker.is_alive && !worker.is_busy && worker.latency_ms < min_latency {
                        min_latency = worker.latency_ms;
                        candidate_idx = Some(i);
                    }
                }
            }
        }

        if let Some(idx) = candidate_idx {
            let worker = &mut workers[idx];
            worker.is_busy = true;
            
            let node_id = worker.address.split(':')
                .next()
                .and_then(|ip| ip.split('.').last())
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(0);

            let hop = HopField {
                node_id,
                latency_ms: worker.latency_ms,
            };
            
            let path_header = PathHeader::new(vec![hop]);
            Some((worker.address.clone(), path_header))
        } else {
            None
        }
    }

    /// タスク実行後にワーカーを解放する
    pub async fn release_worker(&self, address: &str) {
        let mut workers = self.workers.lock().await;
        if let Some(worker) = workers.iter_mut().find(|w| w.address == address) {
            worker.is_busy = false;
            println!("🔓 [RemoteExecutor] ワーカー ( {} ) が解放されました。", address);
        }
    }

    /// 決定された NinjaPacket を伴って【本物の】リモート通信実行を行う
    pub async fn execute_remote(&self, address: &str, mut packet: NinjaPacket) -> Result<(), String> {
        // 1. パケットのフォワード処理（TTL消費など）
        if let Err(e) = packet.forward() {
            return Err(e.to_string());
        }

        let raw_payload = packet.to_bytes();
        let payload_len = raw_payload.len() as u32;

        // 2. TCPコネクションを確立
        let mut stream = TcpStream::connect(address)
            .await
            .map_err(|e| format!("物理接続エラー ({}): {}", address, e))?;

        // 3. プロトコルフレーミング: [4バイトの長さヘッダ] + [パケット本体] を送信
        stream.write_all(&payload_len.to_be_bytes())
            .await
            .map_err(|e| format!("長さヘッダ送信失敗: {}", e))?;
            
        stream.write_all(&raw_payload)
            .await
            .map_err(|e| format!("パケット本体送信失敗: {}", e))?;

        println!(
            "📦 [RemoteExecutor] パケット転送成功 -> 送信先: {} (フレームサイズ: {} bytes, 残りTTL: {})", 
            address, payload_len + 4, packet.ttl
        );

        // 4. ワーカーからの実行完了応答 (ACK) を待機
        // プロトコルとして、正常完了時は 4バイトの "OK\n\n" を受信する設計
        let mut ack_buf = [0u8; 4];
        stream.read_exact(&mut ack_buf)
            .await
            .map_err(|e| format!("ワーカーからの完了応答（ACK）受信失敗: {}", e))?;

        if &ack_buf == b"OK\n\n" {
            Ok(())
        } else {
            Err(format!("ワーカーから不正なレスポンスを受信しました: {:?}", ack_buf))
        }
    }
}