use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tracing::info;

use crate::server::orchestrator::{MasterToWorkerMsg, WorkerToMasterMsg, TaskResult};

pub struct WorkerNode {
    worker_id: String,
    server_addr: String,
}

impl WorkerNode {
    pub fn new(worker_id: impl Into<String>, server_addr: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            server_addr: server_addr.into(),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("🔌 [Worker] Master ({}) に接続を試みています...", self.server_addr);
        let stream = TcpStream::connect(&self.server_addr).await?;
        info!("✅ [Worker] Master への接続に成功しました！");

        // wr は Mutex に入れるため mut は不要です
        let (mut rd, wr) = stream.into_split();
        let wr = Arc::new(tokio::sync::Mutex::new(wr));

        // 1. 登録メッセージの送信
        let reg_msg = WorkerToMasterMsg::Register {
            worker_id: self.worker_id.clone(),
        };
        Self::send_msg(&wr, &reg_msg).await?;

        // 2. ハートビート送信タスクの起動 (5秒間隔)
        let wr_hb = wr.clone();
        let worker_id_hb = self.worker_id.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                let hb_msg = WorkerToMasterMsg::Heartbeat {
                    worker_id: worker_id_hb.clone(),
                };
                if Self::send_msg(&wr_hb, &hb_msg).await.is_err() {
                    break;
                }
            }
        });

        // 3. Masterからのタスク受信ループ
        let mut len_buf = [0u8; 4];
        loop {
            if rd.read_exact(&mut len_buf).await.is_err() {
                info!("⚠️ [Worker] Master との接続が切断されました。");
                break;
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut msg_buf = vec![0u8; len];
            if rd.read_exact(&mut msg_buf).await.is_err() {
                break;
            }

            if let Ok(msg) = serde_json::from_slice::<MasterToWorkerMsg>(&msg_buf) {
                match msg {
                    MasterToWorkerMsg::AssignTask(task) => {
                        info!("🎯 [Worker] タスクを受領しました: task_id={} command={}", task.task_id, task.command);

                        let wr_task = wr.clone();
                        tokio::spawn(async move {
                            let output = Command::new(&task.command)
                                .args(&task.args)
                                .output()
                                .await;

                            let result = match output {
                                Ok(out) => TaskResult {
                                    task_id: task.task_id,
                                    success: out.status.success(),
                                    stdout: String::from_utf8_lossy(&out.stdout).to_string(),
                                    stderr: String::from_utf8_lossy(&out.stderr).to_string(),
                                },
                                Err(e) => TaskResult {
                                    task_id: task.task_id,
                                    success: false,
                                    stdout: String::new(),
                                    stderr: e.to_string(),
                                },
                            };

                            let finish_msg = WorkerToMasterMsg::TaskFinished(result);
                            let _ = Self::send_msg(&wr_task, &finish_msg).await;
                        });
                    }
                    MasterToWorkerMsg::Ping => {}
                }
            }
        }

        Ok(())
    }

    async fn send_msg(
        wr: &Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
        msg: &WorkerToMasterMsg,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bytes = serde_json::to_vec(msg)?;
        let len = (bytes.len() as u32).to_be_bytes();
        let mut guard = wr.lock().await;
        guard.write_all(&len).await?;
        guard.write_all(&bytes).await?;
        Ok(())
    }
}