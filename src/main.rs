// src/main.rs

use std::error::Error;
use ninja::server::Orchestrator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("==================================================");
    println!("🥷  Ninja Orchestrator Server (True Distributed Master) 🥷");
    println!("==================================================");

    // オーケストレーターの初期化と起動 (Worker用: 9001, Client用: 9000)
    let orchestrator = Orchestrator::new("0.0.0.0:9001", "0.0.0.0:9000");
    orchestrator.run().await?;

    Ok(())
}