use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// 通信フレームの最大サイズ (16MB: メモリ爆発・DoS対策)
pub const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// プロトコル層のエラー定義
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("I/O エラー: {0}")]
    Io(#[from] std::io::Error),

    #[error("シリアライズ/デシリアライズエラー: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("フレームサイズ過大: {0} バイト (最大上限: {MAX_FRAME_SIZE} バイト)")]
    FrameTooLarge(u32),

    #[error("相手ノードにより接続が切断されました")]
    ConnectionClosed,
}

/// タスク実行指示のスペック定義
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskSpec {
    pub task_id: String,
    pub command: String,
    pub args: Vec<String>,
}

/// タスク実行結果のスペック定義
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskResultSpec {
    pub task_id: String,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// ノード間で交わされる型安全メッセージ Enum
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Message {
    /// 導通確認 (PING)
    Ping,
    /// 導通応答 (PONG)
    Pong,
    /// Worker から Master への初回登録
    RegisterWorker { worker_id: String },
    /// 定期ハートビート
    Heartbeat { worker_id: String },
    /// Master から Worker へのタスク割当
    TaskAssign(TaskSpec),
    /// Worker から Master へのタスク実行結果返答
    TaskResult(TaskResultSpec),
    /// システムエラー通知
    Error(String),
}

/// メッセージを 4 バイト (Big-Endian) のサイズヘッダー付きで送信
pub async fn send_message<W>(writer: &mut W, msg: &Message) -> Result<(), ProtocolError>
where
    W: AsyncWriteExt + Unpin,
{
    let payload = bincode::serialize(msg)?;
    let payload_len = payload.len() as u32;

    if payload_len > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge(payload_len));
    }

    writer.write_all(&payload_len.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;

    Ok(())
}

/// ストリームから 4 バイトヘッダーを読み取り、メッセージを復元
pub async fn receive_message<R>(reader: &mut R) -> Result<Message, ProtocolError>
where
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    // &mut len_buf で可変参照を渡す
    if let Err(e) = reader.read_exact(&mut len_buf).await {
        if e.kind() == ErrorKind::UnexpectedEof {
            return Err(ProtocolError::ConnectionClosed);
        }
        return Err(ProtocolError::Io(e));
    }

    let payload_len = u32::from_be_bytes(len_buf);
    if payload_len > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge(payload_len));
    }

    let mut payload = vec![0u8; payload_len as usize];
    // &mut payload で可変参照を渡す
    if let Err(e) = reader.read_exact(&mut payload).await {
        if e.kind() == ErrorKind::UnexpectedEof {
            return Err(ProtocolError::ConnectionClosed);
        }
        return Err(ProtocolError::Io(e));
    }

    let msg = bincode::deserialize(&payload)?;
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_send_receive_message_roundtrip() {
        let (mut client, mut server) = duplex(1024);

        let original_msg = Message::TaskAssign(TaskSpec {
            task_id: "task-001".to_string(),
            command: "echo".to_string(),
            args: vec!["Hello Ninja".to_string()],
        });

        send_message(&mut client, &original_msg)
            .await
            .expect("送信失敗");

        let received_msg = receive_message(&mut server)
            .await
            .expect("受信失敗");

        assert_eq!(original_msg, received_msg);
    }
}