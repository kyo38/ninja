use std::collections::{HashMap, VecDeque, HashSet};
use std::time::{Duration, Instant};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use serde::{Serialize, Deserialize};
use tracing::{info, warn, error, info_span, Instrument};

use axum::{
    extract::State,
    response::Html,
    routing::get,
    Json, Router,
};
use tower_http::cors::CorsLayer;

use crate::core::config::Config;

// --- データ構造定義 ---

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
    pub running_tasks: HashSet<String>,
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

    pub fn requeue_tasks(&mut self, task_ids: HashSet<String>) {
        for task_id in task_ids {
            if self.running_tasks.remove(&task_id) {
                warn!("🔄 [Scheduler] 落ちた Worker からタスク [{}] を回収し、再実行キューへ戻しました", task_id);
                self.ready_queue.push_back(task_id);
            }
        }
    }

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

// --- Web API レスポンス用構造体 ---

#[derive(Serialize)]
pub struct ClusterStatusResponse {
    pub active_workers: usize,
    pub has_active_dag: bool,
}

#[derive(Serialize)]
pub struct WorkerInfoResponse {
    pub internal_id: usize,
    pub worker_id: String,
    pub running_task_count: usize,
    pub last_heartbeat_secs_ago: u64,
}

#[derive(Serialize)]
pub struct DagStatusResponse {
    pub total_tasks: usize,
    pub ready_queue_len: usize,
    pub running_tasks_len: usize,
    pub completed_tasks_len: usize,
    pub failed_tasks_len: usize,
    pub is_finished: bool,
}

struct AppState {
    workers: Arc<Mutex<HashMap<usize, WorkerHandle>>>,
    scheduler: Arc<Mutex<Option<DagScheduler>>>,
}

// --- Orchestrator 本体 ---

pub struct Orchestrator {
    worker_addr: String,
    client_addr: String,
    http_addr: String,
    scheduler: Arc<Mutex<Option<DagScheduler>>>,
    workers: Arc<Mutex<HashMap<usize, WorkerHandle>>>,
}

impl Orchestrator {
    pub fn new(worker_addr: impl Into<String>, client_addr: impl Into<String>, http_addr: impl Into<String>) -> Self {
        Self {
            worker_addr: worker_addr.into(),
            client_addr: client_addr.into(),
            http_addr: http_addr.into(),
            scheduler: Arc::new(Mutex::new(None)),
            workers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn from_config(config: Config) -> Self {
        Self::new(config.worker_addr, config.client_addr, "127.0.0.1:8080")
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("=== 🥷 Ninja Distributed Master (Orchestrator) ===");
        info!("📡 サービスポートの設定: worker_addr={} client_addr={} http_addr={}", self.worker_addr, self.client_addr, self.http_addr);
        info!("💡 終了するには 'q' を入力して Enter を押すか、Ctrl + C を押してください。");

        let worker_listener = TcpListener::bind(&self.worker_addr).await?;
        let client_listener = TcpListener::bind(&self.client_addr).await?;

        info!("📡 [Master] Workerからの接続を待機中... (ポート: {})", self.worker_addr);
        info!("📡 [Master] クライアントからのDAGタスク投入を待機中... (ポート: {})", self.client_addr);

        let workers = self.workers.clone();
        let scheduler = self.scheduler.clone();

        // 1. HTTP Web Dashboard API の起動
        let app_state = Arc::new(AppState {
            workers: workers.clone(),
            scheduler: scheduler.clone(),
        });

        let http_addr_parsed: SocketAddr = self.http_addr.parse()?;
        tokio::spawn(async move {
            let app = Router::new()
                .route("/", get(dashboard_html))
                .route("/api/status", get(get_status))
                .route("/api/workers", get(get_workers))
                .route("/api/tasks", get(get_tasks))
                .layer(CorsLayer::permissive())
                .with_state(app_state);

            info!("🌐 [Web Dashboard] ダッシュボードを起動しました: http://{}", http_addr_parsed);
            if let Ok(listener) = tokio::net::TcpListener::bind(http_addr_parsed).await {
                let _ = axum::serve(listener, app).await;
            }
        });

        // 2. Worker受付ループ
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

        // 3. Client受付ループ
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

        // 4. Heartbeat タイムアウト監視ループ (Failover)
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
                        if now.duration_since(handle.last_heartbeat) > Duration::from_secs(12) {
                            dead_workers.push(internal_id);
                        }
                    }
                }

                for id in dead_workers {
                    error!("💀 [Heartbeat Monitor] Worker (internal_id={}) のタイムアウトを検知しました。離脱処理を行います。", id);
                    Self::remove_worker_and_failover(id, workers_hb.clone(), scheduler_hb.clone()).await;
                }
            }
        });

        // stdio 監視
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

        Self::remove_worker_and_failover(internal_id, workers, scheduler).await;
    }

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

// --- Web Dashboard API ハンドラ ---

async fn get_status(State(state): State<Arc<AppState>>) -> Json<ClusterStatusResponse> {
    let workers_guard = state.workers.lock().await;
    let sched_guard = state.scheduler.lock().await;

    Json(ClusterStatusResponse {
        active_workers: workers_guard.len(),
        has_active_dag: sched_guard.is_some(),
    })
}

async fn get_workers(State(state): State<Arc<AppState>>) -> Json<Vec<WorkerInfoResponse>> {
    let workers_guard = state.workers.lock().await;
    let now = Instant::now();

    let list = workers_guard
        .values()
        .map(|w| WorkerInfoResponse {
            internal_id: w.internal_id,
            worker_id: w.worker_id.clone(),
            running_task_count: w.running_tasks.len(),
            last_heartbeat_secs_ago: now.duration_since(w.last_heartbeat).as_secs(),
        })
        .collect();

    Json(list)
}

async fn get_tasks(State(state): State<Arc<AppState>>) -> Json<Option<DagStatusResponse>> {
    let sched_guard = state.scheduler.lock().await;

    if let Some(ref sched) = *sched_guard {
        Json(Some(DagStatusResponse {
            total_tasks: sched.tasks.len(),
            ready_queue_len: sched.ready_queue.len(),
            running_tasks_len: sched.running_tasks.len(),
            completed_tasks_len: sched.completed_tasks.len(),
            failed_tasks_len: sched.failed_tasks.len(),
            is_finished: sched.is_finished(),
        }))
    } else {
        Json(None)
    }
}

/// リアルタイム HTML ダッシュボード
async fn dashboard_html() -> Html<&'static str> {
    Html(r#"
<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>Ninja Cluster Dashboard</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0f172a; color: #f8fafc; margin: 20px; }
        h1 { color: #38bdf8; border-bottom: 2px solid #1e293b; padding-bottom: 10px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-top: 20px; }
        .card { background: #1e293b; border-radius: 8px; padding: 20px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }
        .card h2 { margin-top: 0; color: #94a3b8; font-size: 1.1rem; }
        .metric { font-size: 2rem; font-weight: bold; color: #38bdf8; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { text-align: left; padding: 8px; border-bottom: 1px solid #334155; }
        th { color: #94a3b8; }
        .badge { background: #10b981; color: #022c22; padding: 2px 8px; border-radius: 4px; font-weight: bold; font-size: 0.8rem; }
    </style>
</head>
<body>
    <h1>🥷 Ninja Cluster Live Dashboard</h1>
    <div class="grid">
        <div class="card">
            <h2>Active Workers</h2>
            <div id="worker-count" class="metric">0</div>
        </div>
        <div class="card">
            <h2>DAG Execution Progress</h2>
            <div id="dag-progress" class="metric">N/A</div>
        </div>
    </div>

    <div class="card" style="margin-top: 20px;">
        <h2>Connected Workers</h2>
        <table>
            <thead>
                <tr>
                    <th>Internal ID</th>
                    <th>Worker ID</th>
                    <th>Running Tasks</th>
                    <th>Last Heartbeat</th>
                </tr>
            </thead>
            <tbody id="workers-table"></tbody>
        </table>
    </div>

    <script>
        async function updateDashboard() {
            try {
                const [statusRes, workersRes, tasksRes] = await Promise.all([
                    fetch('/api/status').then(r => r.json()),
                    fetch('/api/workers').then(r => r.json()),
                    fetch('/api/tasks').then(r => r.json())
                ]);

                document.getElementById('worker-count').innerText = statusRes.active_workers;

                if (tasksRes) {
                    document.getElementById('dag-progress').innerText = 
                        `${tasksRes.completed_tasks_len} / ${tasksRes.total_tasks} Completed`;
                } else {
                    document.getElementById('dag-progress').innerText = "Idle";
                }

                const tbody = document.getElementById('workers-table');
                tbody.innerHTML = workersRes.map(w => `
                    <tr>
                        <td>${w.internal_id}</td>
                        <td>${w.worker_id}</td>
                        <td>${w.running_task_count}</td>
                        <td><span class="badge">${w.last_heartbeat_secs_ago}s ago</span></td>
                    </tr>
                `).join('');
            } catch (e) {
                console.error("Failed to fetch cluster state", e);
            }
        }

        setInterval(updateDashboard, 1000);
        updateDashboard();
    </script>
</body>
</html>
    "#)
}