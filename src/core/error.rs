// src/core/error.rs

use thiserror::Error;

#[derive(Error, Debug)]
pub enum NinjaError {
    #[error("IOエラーが発生しました: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSONシリアライズ/デシリアライズエラー: {0}")]
    Json(#[from] serde_json::Error),

    #[error("DAGグラフ構造エラー: {0}")]
    DagError(String),

    #[error("通信エラー: {0}")]
    NetworkError(String),

    #[error("ワーカーエラー: {0}")]
    WorkerError(String),

    #[error("設定読み込みエラー: {0}")]
    ConfigError(String),

    #[error("シャットダウンシグナルを受信しました")]
    Shutdown,
}