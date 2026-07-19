// src/bin/worker.rs

use serde::{Serialize, Deserialize};
use std::error::Error;
use std::env;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::{timeout, Duration};

/// 📦 マネージャーから送られてくるメッセージ（graph.rsの定義と一致）
#[derive(Deserialize)]
enum WorkerMessage {
    HeartbeatPing,
    TaskPayload { name: String, command: String, timeout_secs: u64 },
}

/// 📦 マネージャーへ送り返すレスポンス（graph.rsの定義と一致）
#[derive(Serialize)]
enum WorkerResponse {
    HeartbeatPong,
    TaskResult { name: String, success: bool },
}

/// 🛠️ ワーカー側でのOSプロセス実行コアロジック
async fn execute_command(name: &str, command: &str, timeout_secs: u64) -> bool {
    println!(" ⚡ [Worker] ➔ [{}] Running: {}", name, command);

    let mut cmd = if cfg!(target_os = "windows") {
        let mut c = Command::new("cmd");
        c.args(["/C", command]);
        c
    } else {
        let mut c = Command::new("sh");
        c.args(["-c", command]);
        c
    };

    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ [Worker] ➔ [{}] プロセスの起動に失敗: {:?}", name, e);
            return false;
        }
    };

    let result = if timeout_secs > 0 {
        timeout(Duration::from_secs(timeout_secs), child.wait()).await
    } else {
        Ok(child.wait().await)
    };

    match result {
        Ok(Ok(status)) => {
            if status.success() {
                println!("✓ [Worker] ➔ [{}] 正常終了", name);
                true
            } else {
                eprintln!("❌ [Worker] ➔ [{}] エラー終了 (Exit Code: {:?})", name, status.code());
                false
            }
        }
        Ok(Err(e)) => {
            eprintln!("❌ [Worker] ➔ [{}] プロセス待機中にエラー発生: {:?}", name, e);
            false
        }
        Err(_) => {
            eprintln!("⏱️ [Worker] ➔ [{}] タイムアウトしました ({} 秒制限)", name, timeout_secs);
            let _ = child.kill().await;
            false
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("=== Ninja Remote Worker Node ===");

    // 引数からポート番号を取得。指定がなければ 9000 をデフォルトにする
    let args: Vec<String> = env::args().collect();
    let port = if args.len() > 1 {
        &args[1]
    } else {
        "9000"
    };

    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    println!("📡 [Worker] マネージャーからのメッセージを待機中: {}", addr);

    loop {
        match listener.accept().await {
            Ok((mut stream, manager_addr)) => {
                let mut buffer = vec![0; 65536];
                match stream.read(&mut buffer).await {
                    Ok(0) => continue,
                    Ok(n) => {
                        let json_str = std::str::from_utf8(&buffer[..n])?;
                        if let Ok(msg) = serde_json::from_str::<WorkerMessage>(json_str) {
                            match msg {
                                WorkerMessage::HeartbeatPing => {
                                    // 💓 ハートビートへの即時応答
                                    let response = WorkerResponse::HeartbeatPong;
                                    let serialized = serde_json::to_string(&response)?;
                                    let _ = stream.write_all(serialized.as_bytes()).await;
                                }
                                WorkerMessage::TaskPayload { name, command, timeout_secs } => {
                                    println!("🤝 [Worker] タスクの割り当てを受信しました (Manager: {})", manager_addr);
                                    // 💡 コマンドの実行
                                    let success = execute_command(&name, &command, timeout_secs).await;

                                    // 💡 実行結果をマネージャーへ送り返す
                                    let response = WorkerResponse::TaskResult { name, success };
                                    let serialized = serde_json::to_string(&response)?;
                                    let _ = stream.write_all(serialized.as_bytes()).await;
                                }
                            }
                            let _ = stream.flush().await;
                        }
                    }
                    Err(e) => eprintln!("❌ [Worker] 通信エラー: {:?}", e),
                }
            }
            Err(e) => eprintln!("❌ [Worker] 接続受け入れエラー: {:?}", e),
        }
    }
}