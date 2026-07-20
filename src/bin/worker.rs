// src/bin/worker.rs

use std::error::Error;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// 🛠️ OSプロセスを実行するコアロジック
async fn execute_command(command: &str) -> bool {
    println!("⚡ [Worker] Running command: {}", command);

    let mut cmd = if cfg!(target_os = "windows") {
        let mut c = tokio::process::Command::new("cmd");
        c.args(["/C", command]);
        c
    } else {
        let mut c = tokio::process::Command::new("sh");
        c.args(["-c", command]);
        c
    };

    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ [Worker] プロセスの起動に失敗: {:?}", e);
            return false;
        }
    };

    // 💡 簡単のため、Worker側は一律10秒のタイムアウト制限を設けておきます
    let timeout_secs = 10;
    let result = tokio::time::timeout(tokio::time::Duration::from_secs(timeout_secs), child.wait()).await;

    match result {
        Ok(Ok(status)) => {
            if status.success() {
                println!("✓ [Worker] コマンドが正常に終了しました");
                true
            } else {
                eprintln!("❌ [Worker] コマンドがエラー終了しました (Exit Code: {:?})", status.code());
                false
            }
        }
        Ok(Err(e)) => {
            eprintln!("❌ [Worker] プロセス待機中にエラー発生: {:?}", e);
            false
        }
        Err(_) => {
            eprintln!("⏱️ [Worker] タイムアウトしました ({} 秒制限)", timeout_secs);
            let _ = child.kill().await;
            false
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("=== 🥷 Ninja Distributed Worker (Client Mode) ===");

    // マスターオーケストレーターの待ち受けポート（9001）へ接続
    let master_addr = "127.0.0.1:9001";
    println!("📡 [Worker] マスターサーバー ( {} ) に接続しています...", master_addr);
    
    let mut stream = match TcpStream::connect(master_addr).await {
        Ok(s) => {
            println!("✓ [Worker] マスターに正常に接続しました。指示を待機します。");
            s
        }
        Err(e) => {
            eprintln!("❌ [Worker] マスターへの接続に失敗しました。Masterが起動しているか確認してください: {:?}", e);
            return Err(e.into());
        }
    };

    // マスターからのコマンド要求を受け付けるループ
    let mut buffer = vec![0; 4096];
    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => {
                println!("🛑 [Worker] マスターから接続が切断されました。終了します。");
                break;
            }
            Ok(n) => {
                let command = String::from_utf8_lossy(&buffer[..n]);
                let trimmed_cmd = command.trim();
                
                if trimmed_cmd.is_empty() {
                    continue;
                }

                println!("📥 [Worker] マスターからタスクを受信しました: \"{}\"", trimmed_cmd);
                
                // コマンドを実行
                let success = execute_command(trimmed_cmd).await;
                
                // 結果をマスターに送信 ("SUCCESS" または "FAILED")
                let response = if success { "SUCCESS" } else { "FAILED" };
                if let Err(e) = stream.write_all(response.as_bytes()).await {
                    eprintln!("❌ [Worker] レスポンスの送信に失敗しました: {:?}", e);
                    break;
                }
                let _ = stream.flush().await;
                println!("📤 [Worker] 実行結果 [{}] をマスターに報告しました。\n", response);
            }
            Err(e) => {
                eprintln!("❌ [Worker] 通信エラーが発生しました: {:?}", e);
                break;
            }
        }
    }

    Ok(())
}