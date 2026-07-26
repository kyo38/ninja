use ninja::core::worker::WorkerNode;
use std::env;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ログ出力の初期化 (RUST_LOG=info cargo run --bin worker)
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();

    // コマンドライン引数 または デフォルト値
    let worker_id = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| format!("worker-{}", std::process::id()));

    let server_addr = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:9001".to_string());

    println!("Starting Ninja Worker [{}] -> target: {}", worker_id, server_addr);
    println!("💡 終了するには 'q' を入力して Enter を押すか、Ctrl + C を押してください。");

    let worker = WorkerNode::new(worker_id, server_addr);

    // 'q' キー入力監視用のチャネル
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    // 標準入力監視タスク (Enter付きの 'q' 入力を安定して受け取る)
    std::thread::spawn(move || {
        let mut input = String::new();
        while std::io::stdin().read_line(&mut input).is_ok() {
            if input.trim().eq_ignore_ascii_case("q") {
                let _ = shutdown_tx.blocking_send(());
                break;
            }
            input.clear();
        }
    });

    tokio::select! {
        // Worker 本体の非同期実行ループ
        res = worker.run() => {
            if let Err(e) = res {
                eprintln!("Worker error: {:?}", e);
            }
        }

        // 'q' + Enter による終了検知
        _ = shutdown_rx.recv() => {
            info!("🛑 'q' キー入力を検知しました。Workerを終了します...");
        }

        // Ctrl + C シグナルの監視
        _ = tokio::signal::ctrl_c() => {
            info!("🛑 Ctrl+C を検知しました。Workerを終了します...");
        }
    }

    Ok(())
}