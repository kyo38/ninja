use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskSpec {
    pub task_id: String,
    pub command: String,
    pub args: Vec<String>,
    pub dependencies: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target_addr = "127.0.0.1:9090";
    println!("🚀 Master ({}) へ並列 DAG タスクを送信します...", target_addr);

    let mut stream = TcpStream::connect(target_addr).await?;

    // Windows環境に対応するため cmd /C 経由で echo を実行します
    let tasks = vec![
        TaskSpec {
            task_id: "task-1a".to_string(),
            command: "cmd".to_string(),
            args: vec!["/C".to_string(), "echo Hello from Task 1A (Parallel)".to_string()],
            dependencies: vec![],
        },
        TaskSpec {
            task_id: "task-1b".to_string(),
            command: "cmd".to_string(),
            args: vec!["/C".to_string(), "echo Hello from Task 1B (Parallel)".to_string()],
            dependencies: vec![],
        },
        TaskSpec {
            task_id: "task-2".to_string(),
            command: "cmd".to_string(),
            args: vec!["/C".to_string(), "echo Task 2 executing after 1A and 1B".to_string()],
            dependencies: vec!["task-1a".to_string(), "task-1b".to_string()],
        },
    ];

    let payload = serde_json::to_vec(&tasks)?;
    stream.write_all(&payload).await?;

    let mut response_buf = [0u8; 1024];
    let n = stream.read(&mut response_buf).await?;
    let response = String::from_utf8_lossy(&response_buf[..n]);
    println!("📩 Masterからのレスポンス: {}", response);

    Ok(())
}