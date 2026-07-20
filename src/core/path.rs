#![allow(dead_code)]
#![allow(unused_imports)]

use std::io::{Result, Error, ErrorKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopField {
    pub node_id: u32,
    pub latency_ms: u16,
}

#[derive(Debug, Clone)]
pub struct PathHeader {
    pub hops: Vec<HopField>,
    pub current_index: usize,
}

/// 🥇 パス選択のポリシーを定義
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathStrategy {
    Shortest,    // 最短ホップ（構造用）
    Fastest,     // 最速（低レイテンシ優先）
    Available,   // 空き優先（即時実行重視）
}

impl PathHeader {
    pub fn new(hops: Vec<HopField>) -> Self {
        Self {
            hops,
            current_index: 0,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(self.hops.len() as u8);
        bytes.push(self.current_index as u8);

        for hop in &self.hops {
            bytes.extend(&hop.node_id.to_be_bytes());
            bytes.extend(&hop.latency_ms.to_be_bytes());
        }
        bytes
    }

    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize)> {
        if data.len() < 2 {
            return Err(Error::new(ErrorKind::UnexpectedEof, "ヘッダーの長さが足りません"));
        }

        let hop_count = data[0] as usize;
        let current_index = data[1] as usize;
        let expected_len = 2 + (hop_count * 6);

        if data.len() < expected_len {
            return Err(Error::new(ErrorKind::UnexpectedEof, "パスデータが不完全です"));
        }

        let mut hops = Vec::with_capacity(hop_count);
        let mut pos = 2;

        for _ in 0..hop_count {
            let node_id = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
            let latency_ms = u16::from_be_bytes([data[pos+4], data[pos+5]]);
            hops.push(HopField { node_id, latency_ms });
            pos += 6;
        }

        Ok((Self { hops, current_index }, pos))
    }

    pub fn current_hop(&self) -> Option<&HopField> {
        self.hops.get(self.current_index)
    }

    pub fn increment_hop(&mut self) -> bool {
        if self.current_index + 1 < self.hops.len() {
            self.current_index += 1;
            true
        } else {
            false
        }
    }
}