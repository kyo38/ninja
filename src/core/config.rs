// src/core/config.rs

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub worker_addr: String,
    pub client_addr: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            worker_addr: "127.0.0.1:9001".to_string(),
            client_addr: "127.0.0.1:9090".to_string(),
        }
    }
}