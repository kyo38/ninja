// src/core/mod.rs

pub mod graph;
pub mod executor;
pub mod retry;
pub mod worker;
pub mod packet;
pub mod path;
pub mod error; 
pub mod config; 

pub use error::NinjaError;
pub use config::Config;