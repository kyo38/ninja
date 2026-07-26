use ninja::core::worker::WorkerNode;
use rand::Rng;
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let mut rng = rand::thread_rng();
    let worker_id = format!("worker-{}", rng.gen_range(1000..9999));
    let target_addr = "127.0.0.1:9001";

    println!("Starting Ninja Worker [{}] -> target: {}", worker_id, target_addr);
    println!("💡 終了するには 'q' を入力して Enter を押すか、Ctrl + C を押してください。");

    let worker = WorkerNode::new(worker_id, target_addr);

    // 標準入力を非同期で読み取るための準備
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin).lines();

    // 1. Worker のメインループ
    // 2. キーボード入力 ('q' で終了)
    // 3. Ctrl + C シグナル
    // のいずれかが発生するのを待機
    tokio::select! {
        res = worker.run() => {
            if let Err(e) = res {
                eprintln!("Worker エラー: {}", e);
            }
        }
        _ = async {
            while let Ok(Some(line)) = reader.next_line().await {
                if line.trim().eq_ignore_ascii_case("q") {
                    break;
                }
            }
        } => {
            println!("\n👋 'q' キーが入力されたため、Worker を終了します...");
        }
        _ = tokio::signal::ctrl_c() => {
            println!("\n👋 Ctrl+C を検知したため、Worker を終了します...");
        }
    }

    Ok(())
}