#![allow(dead_code)]

use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;

/// ワーカーの生存状態と負荷状況を管理するセッション構造体
#[derive(Debug, Clone)]
pub struct WorkerSession {
    pub address: String,
    pub active_tasks: usize,
    pub latency_ms: u64,
    pub is_alive: bool,
}

/// 🥇 🔴 WorkerRegistry: ワーカーの状態・負荷管理をカプセル化するコンポーネント
#[derive(Clone)]
pub struct WorkerRegistry {
    sessions: Arc<Mutex<Vec<WorkerSession>>>,
}

impl WorkerRegistry {
    /// アドレスのリストからレジストリを初期化する
    pub fn new(worker_addresses: Vec<String>) -> Self {
        let sessions = worker_addresses
            .into_iter()
            .map(|addr| WorkerSession {
                address: addr,
                active_tasks: 0,
                latency_ms: 9999, // 初期値は大きな値（ペナルティ値）にしておく
                is_alive: true,
            })
            .collect();

        Self {
            sessions: Arc::new(Mutex::new(sessions)),
        }
    }

    /// 戦略（Strategy）アルゴリズムが評価できるように、現在の全セッションのスナップショットを取得する
    pub async fn get_cloned_sessions(&self) -> Vec<WorkerSession> {
        let workers = self.sessions.lock().await;
        workers.clone()
    }

    /// タスク実行開始前に、指定されたワーカーを専有（カウンタをインクリメント）する
    pub async fn acquire(&self, address: &str) -> Result<(), String> {
        let mut workers = self.sessions.lock().await;
        if let Some(w) = workers.iter_mut().find(|w| w.address == address) {
            if !w.is_alive {
                return Err(format!("❌ ワーカー '{}' はダウンしています。", address));
            }
            w.active_tasks += 1;
            println!("🔒 [WorkerRegistry] ワーカー専有: {} (現在の担当タスク数: {})", w.address, w.active_tasks);
            Ok(())
        } else {
            Err(format!("❌ 指定されたワーカー '{}' が見つかりません。", address))
        }
    }

    /// タスク完了時または失敗時に、指定されたワーカーを解放（カウンタをデクリメント）する
    pub async fn release(&self, address: &str) {
        let mut workers = self.sessions.lock().await;
        if let Some(w) = workers.iter_mut().find(|w| w.address == address) {
            if w.active_tasks > 0 {
                w.active_tasks -= 1;
            }
            println!("🔓 [WorkerRegistry] ワーカー解放: {} (現在の担当タスク数: {})", w.address, w.active_tasks);
        }
    }

    /// 定期的なヘルスチェック用：バックグラウンドで全ワーカーのステータスを更新するループを起動
    pub async fn start_heartbeat_loop(&self, interval: Duration) {
        let sessions_clone = Arc::clone(&self.sessions);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let mut workers = sessions_clone.lock().await;
                for worker in workers.iter_mut() {
                    // 本来はここで実際のPingやTCP疎通確認を行う
                    worker.latency_ms = 10; 
                    worker.is_alive = true;
                }
            }
        });
    }
}