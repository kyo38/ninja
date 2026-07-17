use anyhow::Result;
use crate::core::path::PathHeader;

pub const FLAG_SYN: u8 = 0x01;
pub const FLAG_ACK: u8 = 0x02;
pub const FLAG_DATA: u8 = 0x04;
pub const FLAG_PATH: u8 = 0x08; // パス情報が含まれていることを示すフラグ

#[derive(Debug, Clone)]
pub struct NinjaPacket {
    pub version: u8,
    pub flags: u8,
    pub length: u16,
    pub path: Option<PathHeader>, // パスヘッダのフィールドを追加
    pub payload: Vec<u8>,
}

impl NinjaPacket {
    pub fn new(flags: u8, path: Option<PathHeader>, payload: Vec<u8>) -> Self {
        let mut final_flags = flags;
        if path.is_some() {
            final_flags |= FLAG_PATH;
        }

        Self {
            version: 1,
            flags: final_flags,
            length: 0, // to_bytesの中で正確に計算されます
            path,
            payload,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut path_bytes = match &self.path {
            Some(p) => p.to_bytes(),
            None => Vec::new(),
        };

        let total_payload_len = path_bytes.len() + self.payload.len();
        let mut buf = Vec::with_capacity(4 + total_payload_len);

        buf.push(self.version);
        buf.push(self.flags);
        buf.extend_from_slice(&(total_payload_len as u16).to_be_bytes());
        
        buf.append(&mut path_bytes);
        buf.extend_from_slice(&self.payload);

        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            anyhow::bail!("packet too short");
        }

        let version = data[0];
        let flags = data[1];
        let length = u16::from_be_bytes([data[2], data[3]]) as usize;

        if data.len() < 4 + length {
            anyhow::bail!("invalid length");
        }

        let mut offset = 4;
        let mut path = None;

        if flags & FLAG_PATH != 0 {
            let (parsed_path, read_size) = PathHeader::from_bytes(&data[offset..4+length])?;
            path = Some(parsed_path);
            offset += read_size;
        }

        let payload = data[offset..4 + length].to_vec();

        Ok(Self {
            version,
            flags,
            length: length as u16,
            path,
            payload,
        })
    }

    pub fn is_syn(&self) -> bool {
        self.flags & FLAG_SYN != 0
    }

    pub fn is_ack(&self) -> bool {
        self.flags & FLAG_ACK != 0
    }

    pub fn is_data(&self) -> bool {
        self.flags & FLAG_DATA != 0
    }
}