use anyhow::{Result, bail};

/// 各中継ノード（ホップ）が参照する経路情報
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopField {
    pub as_id: u32,       // 自律システム(AS)番号
    pub egress_if: u16,   // 出力インターフェース番号
    pub mac: [u8; 4],     // 経路の正当性を証明する簡易MAC
}

/// パケットに含まれる経路情報全体を管理する構造体
#[derive(Debug, Clone)]
pub struct PathHeader {
    pub current_hop_index: u8, // 現在どこのホップにいるかを示すポインタ
    pub hops: Vec<HopField>,   // 経由するホップのリスト
}

impl PathHeader {
    pub fn new(hops: Vec<HopField>) -> Self {
        Self {
            current_hop_index: 0,
            hops,
        }
    }

    /// バイナリへのシリアライズ
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(self.current_hop_index);
        buf.push(self.hops.len() as u8);

        for hop in &self.hops {
            buf.extend_from_slice(&hop.as_id.to_be_bytes());
            buf.extend_from_slice(&hop.egress_if.to_be_bytes());
            buf.extend_from_slice(&hop.mac);
        }
        buf
    }

    /// バイナリからのパース
    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize)> {
        if data.len() < 2 {
            bail!("Path header too short");
        }
        let current_hop_index = data[0];
        let hop_count = data[1] as usize;
        
        let expected_len = 2 + (hop_count * 10); // 1ホップあたり 4 + 2 + 4 = 10バイト
        if data.len() < expected_len {
            bail!("Invalid path header length");
        }

        let mut hops = Vec::with_capacity(hop_count);
        let mut offset = 2;

        for _ in 0..hop_count {
            let as_id = u32::from_be_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]);
            let egress_if = u16::from_be_bytes([data[offset+4], data[offset+5]]);
            let mut mac = [0u8; 4];
            
            mac.copy_from_slice(&data[offset+6..offset+10]);

            hops.push(HopField { as_id, egress_if, mac });
            offset += 10;
        }

        Ok((Self { current_hop_index, hops }, offset))
    }

    /// 現在のホップ情報を取得
    pub fn current_hop(&self) -> Option<&HopField> {
        self.hops.get(self.current_hop_index as usize)
    }

    /// 次のホップへ進める（すべてのホップを消化して最終目的地を越えたら true を返す）
    pub fn increment_hop(&mut self) -> bool {
        self.current_hop_index += 1;
        // 進めた結果、インデックスがホップ数と同じ（これ以上ホップがない）なら最終目的地に到達したとみなす
        self.current_hop_index as usize >= self.hops.len()
    }
}