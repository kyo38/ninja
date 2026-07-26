use std::collections::{HashMap, VecDeque, HashSet};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use tracing::{info, warn, error, info_span, Instrument};

use crate::core::config::Config;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskSpec {
    pub task_id: String,
    pub command: String,
    pub args: Vec<String>,
    pub dependencies: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskResult {
    pub task_id: String,
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MasterToWorkerMsg {
    AssignTask(TaskSpec),
    Ping,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WorkerToMasterMsg {
    Register { worker_id: String },
    Heartbeat { worker_id: String },
    TaskFinished(TaskResult),
}

/// クラスタ内の Worker 情報と状態
pub struct WorkerHandle {
    pub internal_id: usize,
    pub worker_id: String,
    pub tx: tokio::sync::mpsc::Sender<MasterToWorkerMsg>,
    pub last_heartbeat: Instant,
    pub running_tasks: HashSet<String>, // 実行中のタスクIDリスト
}

/// DAG スケジューラ
pub struct DagScheduler {
    tasks: HashMap<String, TaskSpec>,
    in_degree: HashMap<String, usize>,
    dependents: HashMap<String, Vec<String>>,
    ready_queue: VecDeque<String>,
    completed_tasks: HashSet<String>,
    failed_tasks: HashSet<String>,
    running_tasks: HashSet<String>,
}

impl DagScheduler {
    pub fn new(tasks_vec: Vec<TaskSpec>) -> Self {
        let mut tasks = HashMap::new();
        let mut in_degree = HashMap::new();
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
        let mut ready_queue = VecDeque::new();

        for task in &tasks_vec {
            tasks.insert(task.task_id.clone(), task.clone());
            in_degree.insert(task.task_id.clone(), task.dependencies.len());

            if task.dependencies.is_empty() {
                ready_queue.push_back(task.task_id.clone());
            }

            for dep in &task.dependencies {
                dependents.entry(dep.clone()).or_default().push(task.task_id.clone());
            }
        }

        Self {
            tasks,
            in_degree,
            dependents,
            ready_queue,
            completed_tasks: HashSet::new(),
            failed_tasks: HashSet::new(),
            running_tasks: HashSet::new(),
        }
    }

    pub fn pop_ready_task(&mut self) -> Option<TaskSpec> {
        if let Some(task_id) = self.ready_queue.pop_front() {
            self.running_tasks.insert(task_id.clone());
            self.tasks.get(&task_id).cloned()
        } else {
            None
        }
    }

    /// 落ちた Worker が抱えていた未完了タスクを ready_queue に戻す (Re-queue)
    pub fn requeue_tasks(&mut self, task_ids: HashSet<String>) {
        for task_id in task_ids {
            if self.running_tasks.remove(&task_id) {
                warn!("🔄 [Scheduler] 落ちた Worker からタスク [{}] を回収し、再実行キューへ戻しました", task_id);
                self.ready_queue.push_back(task_id);
            }
        }
    }

    /// タスク完了通知を受け取り、成功した場合のみ後続タスクの依存状態を更新する
    pub fn mark_task_result(&mut self, task_id: &str, success: bool) {
        self.running_tasks.remove(task_id);

        if success {
            self.completed_tasks.insert(task_id.to_string());
            if let Some(deps) = self.dependents.get(task_id) {
                for next_id in deps {
                    if let Some(count) = self.in_degree.get_mut(next_id) {
                        if *count > 0 {
                            *count -= 1;
                            if *count == 0 {
                                info!("🔓 依存関係が解決しました: タスク [{}] が実行可能になりました", next_id);
                                self.ready_queue.push_back(next_id.clone());
                            }
                        }
                    }
                }
            }
        } else {
            self.failed_tasks.insert(task_id.to_string());
            warn!("⚠️ タスク [{}] が失敗したため、これに依存する後続タスクの実行はスキップされます", task_id);
        }
    }

    pub fn is_finished(&self) -> bool {
        (self.completed_tasks.len() + self.failed_tasks.len()) == self.tasks.len()
    }
}

pub struct Orchestrator {
    worker_addr: String,
    client_addr: String,
    scheduler: Arc<Mutex<Option<DagScheduler>>>,
    workers: Arc<Mutex<HashMap<usize, WorkerHandle>>>,
}

impl Orchestrator {
    pub fn new(worker_addr: impl Into<String>, client_addr: impl Into<String>) -> Self {
        Self {
            worker_addr: worker_addr.into(),
            client_addr: client_addr.into(),
            scheduler: Arc::new(Mutex::new(None)),
            workers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn from_config(config: Config) -> Self {
        Self::new(config.worker_addr, config.client_addr)
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("=== 🥷 Ninja Distributed Master (Orchestrator) ===");
        info!("📡 サービスポートの設定を読み込みました worker_addr={} client_addr={}", self.worker_addr, self.client_addr);
        info!("💡 終了するには 'q' を入力して Enter を押すか、Ctrl + C を押してください。");

        let worker_listener = TcpListener::bind(&self.worker_addr).await?;
        let client_listener = TcpListener::bind(&self.client_addr).await?;

        info!("📡 [Master] Workerからの接続を待機中... (ポート: {})", self.worker_addr);
        info!("📡 [Master] クライアントからのDAGタスク投入を待機中... (ポート: {})", self.client_addr);

        let workers = self.workers.clone();
        let scheduler = self.scheduler.clone();

        // 1. Worker受付ループ
        let workers_clone = workers.clone();
        let scheduler_clone = scheduler.clone();
        tokio::spawn(async move {
            let mut internal_id_counter = 0;
            loop {
                if let Ok((socket, addr)) = worker_listener.accept().await {
                    internal_id_counter += 1;
                    let internal_id = internal_id_counter;
                    info!("🤝 [Master] Workerがクラスタに参加しました internal_id={} addr={}", internal_id, addr);

                    let workers_inner = workers_clone.clone();
                    let scheduler_inner = scheduler_clone.clone();

                    tokio::spawn(async move {
                        Self::handle_worker(socket, internal_id, workers_inner, scheduler_inner).await;
                    });
                }
            }
        });

        // 2. Client受付ループ
        let scheduler_clone = scheduler.clone();
        let workers_clone = workers.clone();
        tokio::spawn(async move {
            loop {
                if let Ok((mut socket, addr)) = client_listener.accept().await {
                    info!("📩 クライアントからのDAGタスク要求を受け取りました addr={}", addr);
                    let mut buf = vec![0u8; 65536];
                    if let Ok(n) = socket.read(&mut buf).await {
                        if n > 0 {
                            if let Ok(tasks) = serde_json::from_slice::<Vec<TaskSpec>>(&buf[..n]) {
                                info!("🚀 クライアントから {} 件のタスクを受領し、DAGスケジューラを初期化しました", tasks.len());
                                {
                                    let mut sched_guard = scheduler_clone.lock().await;
                                    *sched_guard = Some(DagScheduler::new(tasks));
                                }
                                let _ = socket.write_all(b"Tasks received").await;
                                Self::dispatch_tasks(scheduler_clone.clone(), workers_clone.clone()).await;
                            }
                        }
                    }
                }
            }
        });

        // 3. Heartbeat タイムアウト監視ループ (フェーズ6: Failover)
        let workers_hb = workers.clone();
        let scheduler_hb = scheduler.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3));
            loop {
                interval.tick().await;
                let mut dead_workers = Vec::new();

                {
                    let workers_guard = workers_hb.lock().await;
                    let now = Instant::now();
                    for (&internal_id, handle) in workers_guard.iter() {
                        // 12秒以上ハートビートが届いていなければ Dead と判定
                        if now.duration_since(handle.last_heartbeat) > Duration::from_secs(12) {
                            dead_workers.push(internal_id);
                        }
                    }
                }

                // 死亡認定された Worker の離脱処理およびタスクの回収・再割り当て
                for id in dead_workers {
                    error!("💀 [Heartbeat Monitor] Worker (internal_id={}) のタイムアウトを検知しました。離脱処理を行います。", id);
                    Self::remove_worker_and_failover(id, workers_hb.clone(), scheduler_hb.clone()).await;
                }
            }
        });

        // stdio 監視（'q' で終了）
        let mut input = String::new();
        while std::io::stdin().read_line(&mut input).is_ok() {
            if input.trim() == "q" {
                info!("👋 シャットダウン要求を受信しました。終了します。");
                break;
            }
            input.clear();
        }

        Ok(())
    }

    async fn handle_worker(
        socket: TcpStream,
        internal_id: usize,
        workers: Arc<Mutex<HashMap<usize, WorkerHandle>>>,
        scheduler: Arc<Mutex<Option<DagScheduler>>>,
    ) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<MasterToWorkerMsg>(32);
        let (mut rd, mut wr) = socket.into_split();

        // 送信タスク
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let Ok(bytes) = serde_json::to_vec(&msg) {
                    let len = (bytes.len() as u32).to_be_bytes();
                    if wr.write_all(&len).await.is_err() { break; }
                    if wr.write_all(&bytes).await.is_err() { break; }
                }
            }
        });

        let mut len_buf = [0u8; 4];
        loop {
            if rd.read_exact(&mut len_buf).await.is_err() { break; }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut msg_buf = vec![0u8; len];
            if rd.read_exact(&mut msg_buf).await.is_err() { break; }

            if let Ok(msg) = serde_json::from_slice::<WorkerToMasterMsg>(&msg_buf) {
                match msg {
                    WorkerToMasterMsg::Register { worker_id } => {
                        info!("📝 Workerが登録されました internal_id={} worker_id={}", internal_id, worker_id);
                        let handle = WorkerHandle {
                            internal_id,
                            worker_id,
                            tx: tx.clone(),
                            last_heartbeat: Instant::now(),
                            running_tasks: HashSet::new(),
                        };
                        workers.lock().await.insert(internal_id, handle);
                    }
                    WorkerToMasterMsg::Heartbeat { worker_id } => {
                        let mut workers_guard = workers.lock().await;
                        if let Some(handle) = workers_guard.get_mut(&internal_id) {
                            handle.last_heartbeat = Instant::now();
                            info!("💓 [Heartbeat] 受信 internal_id={} worker_id={}", internal_id, worker_id);
                        }
                    }
                    WorkerToMasterMsg::TaskFinished(result) => {
                        // 構造化 Span のコンテキスト
                        let result_span = info_span!(
                            "task_result",
                            internal_id = internal_id,
                            task_id = %result.task_id,
                            success = result.success
                        );

                        async {
                            info!(
                                stdout = %result.stdout.trim(),
                                stderr = %result.stderr.trim(),
                                "📥 Workerからタスク実行結果を受信しました"
                            );

                            {
                                let mut workers_guard = workers.lock().await;
                                if let Some(handle) = workers_guard.get_mut(&internal_id) {
                                    handle.running_tasks.remove(&result.task_id);
                                }
                            }

                            let mut finished = false;
                            {
                                let mut sched_guard = scheduler.lock().await;
                                if let Some(ref mut sched) = *sched_guard {
                                    sched.mark_task_result(&result.task_id, result.success);
                                    if sched.is_finished() {
                                        finished = true;
                                    }
                                }
                            }

                            if finished {
                                info!("🎉 [Scheduler] すべての DAG タスクの処理が完了しました！");
                            } else {
                                Self::dispatch_tasks(scheduler.clone(), workers.clone()).await;
                            }
                        }.instrument(result_span).await;
                    }
                }
            }
        }

        // 接続切れ検知時のリロード＆フェイルオーバー
        Self::remove_worker_and_failover(internal_id, workers, scheduler).await;
    }

    /// 死亡・切断された Worker を除去し、未完了タスクを回収して他 Worker へ再割り当て
    async fn remove_worker_and_failover(
        internal_id: usize,
        workers: Arc<Mutex<HashMap<usize, WorkerHandle>>>,
        scheduler: Arc<Mutex<Option<DagScheduler>>>,
    ) {
        let mut abandoned_tasks = HashSet::new();

        {
            let mut workers_guard = workers.lock().await;
            if let Some(handle) = workers_guard.remove(&internal_id) {
                warn!("🚪 Worker (internal_id={} / worker_id={}) をプールから除去しました", internal_id, handle.worker_id);
                abandoned_tasks = handle.running_tasks;
            }
        }

        if !abandoned_tasks.is_empty() {
            let mut sched_guard = scheduler.lock().await;
            if let Some(ref mut sched) = *sched_guard {
                sched.requeue_tasks(abandoned_tasks);
            }
            drop(sched_guard);

            // 残っている生きている Worker へタスクを再分配
            Self::dispatch_tasks(scheduler, workers).await;
        }
    }

    async fn dispatch_tasks(
        scheduler: Arc<Mutex<Option<DagScheduler>>>,
        workers: Arc<Mutex<HashMap<usize, WorkerHandle>>>,
    ) {
        let mut sched_guard = scheduler.lock().await;
        if let Some(ref mut sched) = *sched_guard {
            let mut workers_guard = workers.lock().await;
            if workers_guard.is_empty() { return; }

            for (&internal_id, handle) in workers_guard.iter_mut() {
                if let Some(task) = sched.pop_ready_task() {
                    let dispatch_span = info_span!(
                        "dispatch_task",
                        internal_id = internal_id,
                        worker_id = %handle.worker_id,
                        task_id = %task.task_id
                    );

                    let _guard = dispatch_span.enter();
                    info!("🚀 [Scheduler] タスクを Worker に割り当てます command={}", task.command);

                    handle.running_tasks.insert(task.task_id.clone());
                    let _ = handle.tx.send(MasterToWorkerMsg::AssignTask(task)).await;
                } else {
                    break;
                }
            }
        }
    }
}