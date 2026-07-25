// src/error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NinjaError {
    #[error("I/Oエラー: {0}")]
    Io(#[from] std::io::Error),

    #[error("シリアライズ/デシリアライズ失敗: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("Workerとの接続が切断されました (Addr: {0})")]
    WorkerDisconnected(String),

    #[error("無効なプロトコルメッセージを受信しました")]
    InvalidProtocol,

    #[error("タスク実行タイムアウト (Task ID: {0})")]
    TaskTimeout(u64),

    #[error("チャネル通信エラー: {0}")]
    ChannelError(String),
}

pub type Result<T> = std::result::Result<T, NinjaError>;