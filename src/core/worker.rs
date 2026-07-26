use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use serde::{Serialize, Deserialize};
use tracing::error;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskSpec {
    pub task_id: String,
    pub command: String,
    pub args: Vec<String>,
    pub dependencies: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskResult {
    pub task_id: String,
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MasterToWorkerMsg {
    AssignTask(TaskSpec),
    Ping,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WorkerToMasterMsg {
    Register { worker_id: String },
    Heartbeat { worker_id: String },
    TaskFinished(TaskResult),
}

pub struct Worker {
    worker_id: String,
    target_addr: String,
}

/// 外部コードの `WorkerNode` 参照用エイリアス
pub type WorkerNode = Worker;

impl Worker {
    /// &str と String のどちらの型でも直接渡せるように impl Into<String> を採用
    pub fn new(worker_id: impl Into<String>, target_addr: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            target_addr: target_addr.into(),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Starting Ninja Worker [{}] -> target: {}", self.worker_id, self.target_addr);
        println!("💡 終了するには 'q' を入力して Enter を押すか、Ctrl + C を押してください。");

        let socket = TcpStream::connect(&self.target_addr).await?;
        let (mut rd, mut wr) = socket.into_split();

        // 登録メッセージの送信
        let reg_msg = WorkerToMasterMsg::Register { worker_id: self.worker_id.clone() };
        let bytes = serde_json::to_vec(&reg_msg)?;
        let len = (bytes.len() as u32).to_be_bytes();
        wr.write_all(&len).await?;
        wr.write_all(&bytes).await?;

        // ハートビート送信タスク
        let worker_id_clone = self.worker_id.clone();
        let (hb_tx, mut hb_rx) = tokio::sync::mpsc::channel::<WorkerToMasterMsg>(8);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                let hb = WorkerToMasterMsg::Heartbeat { worker_id: worker_id_clone.clone() };
                if hb_tx.send(hb).await.is_err() { break; }
            }
        });

        // メッセージ受信用チャンネル
        let (tx, mut rx) = tokio::sync::mpsc::channel::<WorkerToMasterMsg>(32);

        // 送信ループ処理
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(msg) = hb_rx.recv() => {
                        if let Ok(bytes) = serde_json::to_vec(&msg) {
                            let len = (bytes.len() as u32).to_be_bytes();
                            if wr.write_all(&len).await.is_err() { break; }
                            if wr.write_all(&bytes).await.is_err() { break; }
                        }
                    }
                    Some(msg) = rx.recv() => {
                        if let Ok(bytes) = serde_json::to_vec(&msg) {
                            let len = (bytes.len() as u32).to_be_bytes();
                            if wr.write_all(&len).await.is_err() { break; }
                            if wr.write_all(&bytes).await.is_err() { break; }
                        }
                    }
                }
            }
        });

        // 受信ループ処理
        let mut len_buf = [0u8; 4];
        loop {
            if rd.read_exact(&mut len_buf).await.is_err() { break; }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut msg_buf = vec![0u8; len];
            if rd.read_exact(&mut msg_buf).await.is_err() { break; }

            if let Ok(msg) = serde_json::from_slice::<MasterToWorkerMsg>(&msg_buf) {
                match msg {
                    MasterToWorkerMsg::AssignTask(task) => {
                        let tx_clone = tx.clone();
                        let worker_id = self.worker_id.clone();
                        tokio::spawn(async move {
                            let result = Self::execute_task(&worker_id, task).await;
                            let _ = tx_clone.send(WorkerToMasterMsg::TaskFinished(result)).await;
                        });
                    }
                    MasterToWorkerMsg::Ping => {}
                }
            }
        }

        Ok(())
    }

    async fn execute_task(worker_id: &str, task: TaskSpec) -> TaskResult {
        #[cfg(target_os = "windows")]
        let mut cmd = {
            let mut c = Command::new("cmd");
            let full_cmd = format!("chcp 65001 >nul && {} {}", task.command, task.args.join(" "));
            c.args(&["/C", &full_cmd]);
            c
        };

        #[cfg(not(target_os = "windows"))]
        let mut cmd = {
            let mut c = Command::new(&task.command);
            c.args(&task.args);
            c
        };

        match cmd.output().await {
            Ok(output) => {
                let success = output.status.success();
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                TaskResult {
                    task_id: task.task_id,
                    success,
                    stdout,
                    stderr,
                }
            }
            Err(e) => {
                error!("[Worker:{}] Failed to spawn process for task {}: {:?}", worker_id, task.task_id, e);
                TaskResult {
                    task_id: task.task_id,
                    success: false,
                    stdout: String::new(),
                    stderr: format!("Failed to execute command: {}", e),
                }
            }
        }
    }
}