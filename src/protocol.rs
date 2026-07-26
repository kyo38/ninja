use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use thiserror::Error;

/// プロトコル処理に関するエラー
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("IOエラー: {0}")]
    Io(#[from] std::io::Error),

    #[error("シリアライズ/デシリアライズエラー: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("接続が切断されました")]
    ConnectionClosed,
}

/// タスク定義仕様
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskSpec {
    pub task_id: String,
    pub command: String,
    pub args: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default)]
    pub max_retries: u32,
}

fn default_timeout() -> u64 {
    30
}

/// タスク実行結果仕様
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskResultSpec {
    pub task_id: String,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// クライアント・ワーカー・サーバー間をやり取りする共通メッセージ定義
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// クライアントからのタスク投入
    SubmitTasks(Vec<TaskSpec>),
    /// ワーカー登録
    RegisterWorker { worker_id: String },
    /// ハートビート
    Heartbeat { worker_id: String },
    /// ワーカーへのタスク割り当て
    TaskAssign(TaskSpec),
    /// タスク実行結果の通知
    TaskResult(TaskResultSpec),
    /// 接続確認・応答
    Ping,
    Pong,
}

/// 4バイトのペイロード長ヘッダーを付けて JSON メッセージを送信する
/// TcpStream だけでなく OwnedWriteHalf 等でも動作するよう AsyncWrite に汎用化
pub async fn send_message<W>(stream: &mut W, msg: &Message) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    let payload = serde_json::to_vec(msg)?;
    let length = payload.len() as u32;

    // 長さ（BigEndian 4バイト）を送信
    stream.write_all(&length.to_be_bytes()).await?;
    // ペイロード本体を送信
    stream.write_all(&payload).await?;
    stream.flush().await?;

    Ok(())
}

/// 4バイトの長さヘッダーを読み取り、JSON メッセージとして復元する
/// TcpStream だけでなく OwnedReadHalf 等でも動作するよう AsyncRead に汎用化
pub async fn receive_message<R>(stream: &mut R) -> Result<Message, ProtocolError>
where
    R: AsyncRead + Unpin,
{
    let mut len_bytes = [0u8; 4];

    if let Err(e) = stream.read_exact(&mut len_bytes).await {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Err(ProtocolError::ConnectionClosed);
        }
        return Err(ProtocolError::Io(e));
    }

    let length = u32::from_be_bytes(len_bytes) as usize;
    let mut buffer = vec![0u8; length];
    stream.read_exact(&mut buffer).await?;

    let msg: Message = serde_json::from_slice(&buffer)?;
    Ok(msg)
}