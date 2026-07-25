pub mod core;
pub mod error;
pub mod platform;
pub mod server;
pub mod transport;

// よく使う型をトップレベルに re-export しておくと便利です
pub use error::{NinjaError, Result};