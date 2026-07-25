// src/server/orchestrator.rs

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, Notify};
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

use crate::core::config::Config;
use crate::core::graph::{DagScheduler, Task, TaskState};
use crate::core::worker::WorkerRegistry;
use crate::server::client_handler::ClientHandler;
use crate::server::worker_pool::WorkerPool;

pub struct Orchestrator {
    config: Config,
    worker_pool: Arc<WorkerPool>,
    worker_registry: Arc<WorkerRegistry>,
    task_queue: Arc<Mutex<Vec<Task>>>,
    pulse: Arc<Notify>,
}

impl Orchestrator {
    pub fn from_config(config: Config) -> Self {
        let pulse = Arc::new(Notify::new());
        let worker_pool = Arc::new(WorkerPool::new(Arc::clone(&pulse)));
        let worker_registry = Arc::new(WorkerRegistry::new(vec![config.worker_addr.clone()]));

        Self {
            config,
            worker_pool,
            worker_registry,
            task_queue: Arc::new(Mutex::new(Vec::new())),
            pulse,
        }
    }

    pub fn new(
        config: Config,
        worker_pool: Arc<WorkerPool>,
        worker_registry: Arc<WorkerRegistry>,
        pulse: Arc<Notify>,
    ) -> Self {
        Self {
            config,
            worker_pool,
            worker_registry,
            task_queue: Arc::new(Mutex::new(Vec::new())),
            pulse,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn Error>> {
        // 0. WorkerRegistry のヘルスチェック
        self.worker_registry
            .start_heartbeat_loop(Duration::from_secs(5))
            .await;

        // 1. Worker 接続リスナー
        let wp = Arc::clone(&self.worker_pool);
        let worker_addr = self.config.worker_addr.clone();
        tokio::spawn(async move {
            if let Err(e) = wp.start_listener(&worker_addr).await {
                error!("❌ [Master] WorkerPool エラー: {:?}", e);
            }
        });

        // 2. Client タスク受信リスナー
        let client_addr = self.config.client_addr.clone();
        let tq_client = Arc::clone(&self.task_queue);
        let pulse_client = Arc::clone(&self.pulse);

        tokio::spawn(async move {
            match ClientHandler::bind(&client_addr).await {
                Ok(mut handler) => loop {
                    match handler.accept_tasks().await {
                        Ok(new_tasks) => {
                            let mut queue = tq_client.lock().await;
                            queue.extend(new_tasks);
                            info!("🚀 [Master] 新しい DAG タスク群をキューに追加しました。");
                            pulse_client.notify_waiters();
                        }
                        Err(e) => {
                            error!("❌ [Master] クライアントタスク受信エラー: {:?}", e);
                            sleep(Duration::from_millis(500)).await;
                        }
                    }
                },
                Err(e) => {
                    error!("❌ [Master] ClientHandler バインドエラー: {:?}", e);
                }
            }
        });

        // 3. DAG スケジューラーループ
        let tq_sched = Arc::clone(&self.task_queue);
        let registry_sched = Arc::clone(&self.worker_registry);
        let pulse_sched = Arc::clone(&self.pulse);
        let wp_sched = Arc::clone(&self.worker_pool);

        tokio::spawn(async move {
            Self::schedule_loop(tq_sched, registry_sched, pulse_sched, wp_sched).await;
        });

        info!("💡 終了するには 'q' を入力して Enter を押すか、Ctrl + C を押してください。");

        // 4. シャットダウン監視
        let mut stdin = tokio::io::stdin();
        let mut buf = [0u8; 1024];

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("🛑 [Master] Ctrl+C を検知しました。Orchestrator をシャットダウンします...");
                    break;
                }
                res = stdin.read(&mut buf) => {
                    match res {
                        Ok(n) if n > 0 => {
                            let input = String::from_utf8_lossy(&buf[..n]);
                            if input.trim().eq_ignore_ascii_case("q") {
                                info!("🛑 [Master] 'q' キー入力を検知しました。Orchestrator をシャットダウンします...");
                                break;
                            }
                        }
                        _ => {
                            sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn schedule_loop(
        task_queue: Arc<Mutex<Vec<Task>>>,
        registry: Arc<WorkerRegistry>,
        pulse: Arc<Notify>,
        worker_pool: Arc<WorkerPool>,
    ) {
        let mut task_states: HashMap<String, TaskState> = HashMap::new();
        let mut scheduler: Option<DagScheduler> = None;
        let completed_tasks = Arc::new(Mutex::new(Vec::<(String, bool)>::new())); // (TaskName, SuccessFlag)

        loop {
            let _ = tokio::time::timeout(Duration::from_millis(200), pulse.notified()).await;

            // 完了したタスクの状態反映
            {
                let mut done_list = completed_tasks.lock().await;
                for (task_name, is_success) in done_list.drain(..) {
                    if is_success {
                        task_states.insert(task_name, TaskState::Success);
                    } else {
                        task_states.insert(task_name, TaskState::Failed);
                    }
                }
            }

            let mut tasks = task_queue.lock().await;
            if tasks.is_empty() {
                continue;
            }

            if scheduler.is_none() {
                match DagScheduler::new(tasks.clone()) {
                    Ok(sched) => {
                        info!("⚙️  [Master] DAG スケジューラーを初期化しました。");
                        for t in tasks.iter() {
                            task_states.insert(t.name.clone(), TaskState::Pending);
                        }
                        scheduler = Some(sched);
                    }
                    Err(e) => {
                        error!("❌ [Master] DAG 構築エラー: {}", e);
                        tasks.clear();
                        continue;
                    }
                }
            }

            let sched = scheduler.as_ref().unwrap();
            let ready_tasks = sched.get_ready_tasks(&task_states);

            if ready_tasks.is_empty() {
                let all_finished = !task_states.is_empty()
                    && task_states.values().all(|s| matches!(s, TaskState::Success | TaskState::Failed));

                if all_finished {
                    info!("🎉 [Master] すべての DAG タスクの実行が完了しました！");
                    tasks.clear();
                    task_states.clear();
                    scheduler = None;
                }
                continue;
            }

            for task_name in ready_tasks {
                let sessions = registry.get_cloned_sessions().await;
                let target_worker = sessions.iter().find(|w| w.is_alive).map(|w| w.address.clone());

                if let Some(worker_addr) = target_worker {
                    if let Some(task) = tasks.iter().find(|t| t.name == task_name).cloned() {
                        if registry.acquire(&worker_addr).await.is_ok() {
                            info!(
                                "✈️  [Master] タスク [{}] を Worker ({}) に割り当てます (コマンド: '{}')",
                                task.name, worker_addr, task.command
                            );

                            task_states.insert(task.name.clone(), TaskState::Running { worker_id: 1 });

                            let registry_clone = Arc::clone(&registry);
                            let pulse_clone = Arc::clone(&pulse);
                            let completed_tasks_clone = Arc::clone(&completed_tasks);
                            let wp_clone = Arc::clone(&worker_pool);
                            let name_clone = task.name.clone();
                            let cmd_clone = task.command.clone();

                            // 実際の TcpStream 経由で Worker へ送信してレスポンスを待つ
                            tokio::spawn(async move {
                                let is_success = match wp_clone.send_command(&cmd_clone).await {
                                    Ok(res) => {
                                        info!("✓ [Master] タスク [{}] 実行完了。応答: {}", name_clone, res);
                                        true
                                    }
                                    Err(e) => {
                                        error!("❌ [Master] タスク [{}] 実行失敗: {:?}", name_clone, e);
                                        false
                                    }
                                };

                                {
                                    let mut done = completed_tasks_clone.lock().await;
                                    done.push((name_clone, is_success));
                                }

                                registry_clone.release(&worker_addr).await;
                                pulse_clone.notify_waiters();
                            });
                        }
                    }
                } else {
                    warn!("⚠️  [Master] 割り当て可能な Worker がありません。リトライを待機します。");
                    break;
                }
            }
        }
    }
}