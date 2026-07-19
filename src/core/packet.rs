#![allow(dead_code)]
#![allow(unused_imports)]

use crate::core::path::PathHeader;
use std::io::{Result, Error, ErrorKind};

pub const FLAG_SYN: u8 = 0x01;
pub const FLAG_ACK: u8 = 0x02;
pub const FLAG_DATA: u8 = 0x04;
pub const FLAG_PATH: u8 = 0x08;

#[derive(Debug, Clone)]
pub struct NinjaPacket {
    pub flags: u8,
    pub path: Option<PathHeader>,
    pub payload: Vec<u8>,
}

impl NinjaPacket {
    pub fn new(flags: u8, path: Option<PathHeader>, payload: Vec<u8>) -> Self {
        let mut actual_flags = flags;
        if path.is_some() {
            actual_flags |= FLAG_PATH;
        }
        Self {
            flags: actual_flags,
            path,
            payload,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(self.flags);

        if let Some(ref path_header) = self.path {
            bytes.extend(path_header.to_bytes());
        }

        bytes.extend(&self.payload);
        bytes
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "データが空です"));
        }

        let flags = data[0];
        let mut current_pos = 1;
        let mut path = None;

        if (flags & FLAG_PATH) != 0 {
            let (header, consumed) = PathHeader::from_bytes(&data[current_pos..])?;
            path = Some(header);
            current_pos += consumed;
        }

        let payload = data[current_pos..].to_vec();

        Ok(Self { flags, path, payload })
    }

    pub fn is_syn(&self) -> bool {
        (self.flags & FLAG_SYN) != 0
    }

    pub fn is_ack(&self) -> bool {
        (self.flags & FLAG_ACK) != 0
    }

    pub fn is_data(&self) -> bool {
        (self.flags & FLAG_DATA) != 0
    }
}