#![allow(dead_code)]

use crate::core::worker::WorkerSession;

/// ネットワーク転送用のルーティングヘッダー
#[derive(Debug, Clone)]
pub struct PathHeader {
    pub routes: Vec<String>,
    pub current_hop: usize,
}

impl PathHeader {
    pub fn new(routes: Vec<String>) -> Self {
        Self {
            routes,
            current_hop: 0,
        }
    }

    /// パケットのシリアライズ用
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(self.current_hop as u32).to_be_bytes());
        bytes.extend_from_slice(&(self.routes.len() as u32).to_be_bytes());
        
        for route in &self.routes {
            let route_bytes = route.as_bytes();
            bytes.extend_from_slice(&(route_bytes.len() as u32).to_be_bytes());
            bytes.extend_from_slice(route_bytes);
        }
        bytes
    }

    /// パケットのデシリアライズ用 (std::io::Error を返すように修正)
    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize), std::io::Error> {
        if data.len() < 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "PathHeaderのパケット長が足りません",
            ));
        }

        let current_hop = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let routes_count = u32::from_be_bytes([data[4], data[5], data[6], data[7]]) as usize;
        
        let mut offset = 8;
        let mut routes = Vec::new();

        for _ in 0..routes_count {
            if data.len() < offset + 4 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "PathHeaderのルート長データが破損しています",
                ));
            }
            let route_len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if data.len() < offset + route_len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "PathHeaderのルート文字列データが不足しています",
                ));
            }
            let route_str = String::from_utf8(data[offset..offset + route_len].to_vec())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            routes.push(route_str);
            offset += route_len;
        }

        Ok((Self { routes, current_hop }, offset))
    }

    /// ホップ数を進めるメソッド
    pub fn increment_hop(&mut self) {
        self.current_hop += 1;
    }
}

/// 利用可能なワーカーセッションから、戦略に基づいて最適なワーカーを選択する
pub trait PathStrategy: Send + Sync {
    fn select_path(&self, workers: &[WorkerSession]) -> Result<String, String>;
}

/// 最小負荷（アクティブタスク数 + レイテンシ）を選択する具体的な戦略
pub struct LeastLoadStrategy;

impl PathStrategy for LeastLoadStrategy {
    fn select_path(&self, workers: &[WorkerSession]) -> Result<String, String> {
        let chosen = workers
            .iter()
            .filter(|w| w.is_alive)
            .min_by_key(|w| w.active_tasks * 10 + (w.latency_ms as usize));

        if let Some(worker) = chosen {
            Ok(worker.address.clone())
        } else {
            Err("❌ 有効な稼働中のワーカーが見つかりません（LeastLoadStrategy）。".to_string())
        }
    }
}