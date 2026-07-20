#![allow(dead_code)]

use std::io::{Result, Error, ErrorKind};
use crate::core::path::PathHeader;

/// 🥈 SCIONライクな通信パケット構造体
#[derive(Debug, Clone)]
pub struct NinjaPacket {
    pub ttl: u8,               // 🥈 無限ループ防止用（Time To Live）
    pub path: PathHeader,      // 経路情報（HopFieldのリスト）
    pub payload: Vec<u8>,      // 実際のデータ（シリアライズされたコマンドなど）
}

impl NinjaPacket {
    pub fn new(ttl: u8, path: PathHeader, payload: Vec<u8>) -> Self {
        Self { ttl, path, payload }
    }

    /// パケット全体をバイト列にシリアライズ
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        
        // 1. TTLを1バイト書き込み
        bytes.push(self.ttl);

        // 2. パスヘッダーをシリアライズして追加
        let path_bytes = self.path.to_bytes();
        bytes.extend(path_bytes);

        // 3. ペイロード（データ本体）を追加
        bytes.extend(&self.payload);

        bytes
    }

    /// バイト列からパケット構造体をデシリアライズ
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "パケットデータが空です"));
        }

        // 1. TTLの読み込み
        let ttl = data[0];

        // 2. パスヘッダーの読み込み
        let (path, offset) = PathHeader::from_bytes(&data[1..])?;
        
        // offsetは &data[1..] からの相対位置なので、全体のインデックスは 1 + offset
        let payload_pos = 1 + offset;
        if data.len() < payload_pos {
            return Err(Error::new(ErrorKind::UnexpectedEof, "ペイロードデータが不完全です"));
        }

        // 3. ペイロードの抽出
        let payload = data[payload_pos..].to_vec();

        Ok(Self { ttl, path, payload })
    }

    /// 🥈 ホップを1つ進め、TTLをデクリメントする
    /// TTLが0になった場合はエラーを返す
    pub fn forward(&mut self) -> Result<()> {
        if self.ttl <= 1 {
            return Err(Error::new(ErrorKind::Other, "❌ [Packet] TTL expired (Time To Live が0になりました。パケットを破棄します)"));
        }
        self.ttl -= 1;
        self.path.increment_hop();
        Ok(())
    }
}