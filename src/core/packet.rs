use anyhow::Result;

pub const FLAG_SYN: u8 = 0x01;
pub const FLAG_ACK: u8 = 0x02;
pub const FLAG_DATA: u8 = 0x04;

#[derive(Debug, Clone)]
pub struct NinjaPacket {
    pub version: u8,
    pub flags: u8,
    pub length: u16,
    pub payload: Vec<u8>,
}

impl NinjaPacket {
    pub fn new(flags: u8, payload: Vec<u8>) -> Self {
        Self {
            version: 1,
            flags,
            length: payload.len() as u16,
            payload,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + self.payload.len());

        buf.push(self.version);
        buf.push(self.flags);
        buf.extend_from_slice(&self.length.to_be_bytes());
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

        let payload = data[4..4 + length].to_vec();

        Ok(Self {
            version,
            flags,
            length: length as u16,
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