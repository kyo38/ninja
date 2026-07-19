// src/core/graph.rs

use serde::{Serialize, Deserialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub name: String,
    pub deps: Vec<String>,
    pub command: String,
    pub timeout_secs: u64,
    pub max_retries: usize,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Done,
    Failed,
}

#[derive(Debug, PartialEq)]
pub enum SchedulerError {
    CycleDetected,
    UnknownDependency(String),
}

/// 📦 ワーカーへ送信するペイロード構造体 (worker.rsの定義と一致)
#[derive(Serialize)]
struct WorkerTaskPayload {
    name: String,
    command: String,
    timeout_secs: u64,
}

/// 📦 ワーカーから返ってくるレスポンス構造体
#[derive(Deserialize)]
struct WorkerResponse {
    name: String,
    success: bool,
}

/// 🌐 ネットワーク経由でリモートワーカーに処理を Push する Executor
pub struct RemoteExecutor {
    // 利用可能なワーカーのIP:PORTリストと、それぞれの使用中フラグを一元管理
    // マネージャー主導（Push型）で空きスロットを探すためにスレッドセーフにする
    workers: Arc<Mutex<Vec<(String, bool)>>>,
}

impl RemoteExecutor {
    pub fn new(worker_addresses: Vec<String>) -> Self {
        let workers = worker_addresses.into_iter().map(|addr| (addr, false)).collect();
        RemoteExecutor {
            workers: Arc::new(Mutex::new(workers)),
        }
    }

    /// 空いているワーカーを1つ確保する (見つかるまでループ)
    async fn acquire_worker(&self) -> String {
        loop {
            let mut workers = self.workers.lock().await;
            for (addr, is_busy) in workers.iter_mut() {
                if !*is_busy {
                    *is_busy = true; // 使用中にマーク
                    return addr.clone();
                }
            }
            drop(workers);
            // 空きがない場合は少し待ってから再試行 (ポーリング)
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    }

    /// 使い終わったワーカーを解放する
    async fn release_worker(&self, target_addr: &str) {
        let mut workers = self.workers.lock().await;
        for (addr, is_busy) in workers.iter_mut() {
            if addr == target_addr {
                *is_busy = false;
                break;
            }
        }
    }
}

/// 🌐 非同期対応したExecutorトレイト
#[async_trait::async_trait]
pub trait Executor: Send + Sync {
    async fn execute(&self, task: &Task) -> bool;
}

#[async_trait::async_trait]
impl Executor for RemoteExecutor {
    async fn execute(&self, task: &Task) -> bool {
        let mut attempts = 0;
        let max_attempts = task.max_retries + 1;

        while attempts < max_attempts {
            attempts += 1;
            if attempts > 1 {
                println!("🔄 [RemoteExecute] ➔ [{}] リトライを開始します ({}/{})", task.name, attempts - 1, task.max_retries);
            }

            // 1. マネージャー主導で、現在空いているワーカーのルート（パス）を決定して確保
            let worker_addr = self.acquire_worker().await;
            println!("🚀 [RemoteExecute] ➔ [{}] ワーカー( {} ) を割り当てました。タスクを Push 送信します...", task.name, worker_addr);

            // 2. ワーカーへTCP接続を確立
            let mut stream = match TcpStream::connect(&worker_addr).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("❌ [RemoteExecute] ➔ ワーカー( {} ) への接続に失敗しました: {:?}", worker_addr, e);
                    self.release_worker(&worker_addr).await;
                    continue; // 次の試行（リトライ）へ
                }
            };

            // 3. ペイロードの組み立てとシリアライズ
            let payload = WorkerTaskPayload {
                name: task.name.clone(),
                command: task.command.clone(),
                timeout_secs: task.timeout_secs,
            };

            let serialized = match serde_json::to_string(&payload) {
                Ok(json) => json,
                Err(e) => {
                    eprintln!("❌ [RemoteExecute] ➔ ペイロードのJSON化に失敗: {:?}", e);
                    self.release_worker(&worker_addr).await;
                    return false;
                }
            };

            // 4. ワーカーへコマンドデータを送信 (Push)
            if let Err(e) = stream.write_all(serialized.as_bytes()).await {
                eprintln!("❌ [RemoteExecute] ➔ ワーカーへのデータ送信失敗: {:?}", e);
                self.release_worker(&worker_addr).await;
                continue;
            }
            let _ = stream.flush().await;

            // 5. ワーカーからの実行結果の受信待機
            let mut buffer = vec![0; 65536];
            let result = match stream.read(&mut buffer).await {
                Ok(0) => {
                    eprintln!("❌ [RemoteExecute] ➔ ワーカーが結果を返さずに接続を切断しました。");
                    false
                }
                Ok(n) => {
                    if let Ok(json_str) = std::str::from_utf8(&buffer[..n]) {
                        if let Ok(resp) = serde_json::from_str::<WorkerResponse>(json_str) {
                            if resp.success {
                                println!("✓ [RemoteExecute] ➔ [{}] ワーカー側で正常終了", task.name);
                                true
                            } else {
                                eprintln!("❌ [RemoteExecute] ➔ [{}] ワーカー側でエラーまたはタイムアウト終了", task.name);
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                }
                Err(e) => {
                    eprintln!("❌ [RemoteExecute] ➔ ワーカーからの結果受信中にエラー発生: {:?}", e);
                    false
                }
            };

            // 6. タスク処理が終わったので、ワーカーをプールに返却
            self.release_worker(&worker_addr).await;

            if result {
                return true; // 成功したため終了
            }
        }

        false // すべてのリトライが失敗
    }
}

/// 📦 アクター（メインループ）に送るメッセージの定義
enum Event {
    TaskFinished(String, bool),
}

/// 🛡️ チャネル駆動型 DagScheduler
pub struct DagScheduler {
    tasks: HashMap<String, Task>,
    adjacency_list: HashMap<String, Vec<String>>,
    initial_indegrees: HashMap<String, usize>,
    initial_statuses: HashMap<String, TaskStatus>,
    initial_ready_tasks: Vec<String>,
}

impl DagScheduler {
    pub fn new(received_tasks: Vec<Task>) -> Result<Self, SchedulerError> {
        let mut tasks = HashMap::new();
        let mut base_indegrees = HashMap::new();
        let mut adjacency_list = HashMap::new();
        let mut statuses = HashMap::new();

        for task in &received_tasks {
            let name = task.name.clone();
            base_indegrees.insert(name.clone(), 0);
            adjacency_list.insert(name.clone(), Vec::new());
            statuses.insert(name.clone(), TaskStatus::Pending);
            tasks.insert(name, task.clone());
        }

        for task in &received_tasks {
            for dep in &task.deps {
                if !tasks.contains_key(dep) {
                    return Err(SchedulerError::UnknownDependency(dep.clone()));
                }
                if let Some(list) = adjacency_list.get_mut(dep) {
                    list.push(task.name.clone());
                }
                if let Some(deg) = base_indegrees.get_mut(&task.name) {
                    *deg += 1;
                }
            }
        }

        // サイクル検出 (トポロジカルソートの検証)
        let mut temp_indegrees = base_indegrees.clone();
        let mut validation_queue = VecDeque::new();
        for (name, &deg) in &temp_indegrees {
            if deg == 0 {
                validation_queue.push_back(name.clone());
            }
        }

        let mut sorted_count = 0;
        let mut initial_ready_tasks = Vec::new();
        while let Some(u) = validation_queue.pop_front() {
            sorted_count += 1;
            if *temp_indegrees.get(&u).unwrap() == 0 {
                if *base_indegrees.get(&u).unwrap() == 0 {
                    initial_ready_tasks.push(u.clone());
                }
            }
            if let Some(followers) = adjacency_list.get(&u) {
                for follower in followers {
                    if let Some(deg) = temp_indegrees.get_mut(follower) {
                        *deg -= 1;
                        if *deg == 0 {
                            validation_queue.push_back(follower.clone());
                        }
                    }
                }
            }
        }

        if sorted_count != received_tasks.len() {
            return Err(SchedulerError::CycleDetected);
        }

        Ok(DagScheduler {
            tasks,
            adjacency_list,
            initial_indegrees: base_indegrees,
            initial_statuses: statuses,
            initial_ready_tasks,
        })
    }

    /// 🔄 完全メッセージ駆動のメインループ
    pub async fn run(self, executor: Arc<dyn Executor>) {
        println!("🚀 [Ninja Engine] --- ロックフリー・チャネル駆動タスクループ開始 ---");

        let mut indegrees = self.initial_indegrees;
        let mut statuses = self.initial_statuses;
        let mut running_count = 0;
        let mut has_failed = false;

        let (event_tx, mut event_rx) = mpsc::channel::<Event>(1024);
        let mut ready_queue = VecDeque::from(self.initial_ready_tasks);

        loop {
            if !has_failed {
                while let Some(task_name) = ready_queue.pop_front() {
                    let task = self.tasks.get(&task_name).unwrap().clone();
                    let event_tx_clone = event_tx.clone();
                    let exec_clone = Arc::clone(&executor);

                    statuses.insert(task_name.clone(), TaskStatus::Running);
                    running_count += 1;

                    tokio::spawn(async move {
                        let success = exec_clone.execute(&task).await;
                        let _ = event_tx_clone.send(Event::TaskFinished(task.name, success)).await;
                    });
                }
            } else {
                ready_queue.clear();
            }

            if running_count == 0 && ready_queue.is_empty() {
                break;
            }

            if let Some(event) = event_rx.recv().await {
                match event {
                    Event::TaskFinished(finished_task, success) => {
                        running_count -= 1;

                        if !success {
                            has_failed = true;
                            statuses.insert(finished_task.clone(), TaskStatus::Failed);
                            println!("❌ [Ninja Engine] タスク [{}] が最終的に失敗しました。新規の発火を完全に停止します。", finished_task);
                            continue;
                        }

                        statuses.insert(finished_task.clone(), TaskStatus::Done);

                        if !has_failed {
                            if let Some(followers) = self.adjacency_list.get(&finished_task) {
                                for follower in followers {
                                    if let Some(deg) = indegrees.get_mut(follower) {
                                        *deg -= 1;
                                        if *deg == 0 {
                                            ready_queue.push_back(follower.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if has_failed {
            println!("❌ [Ninja Engine] 一部タスクのエラーにより、実行が中断されました。");
        } else {
            println!("🎉 [Ninja Engine] 全てのタスクグラフが依存関係通りに完全実行されました。\n");
        }
    }
}

pub fn resolve_execution_order(tasks: &[Task]) -> Result<Vec<String>, String> {
    let mut indegree = HashMap::new();
    let mut adj = HashMap::new();
    for task in tasks {
        indegree.insert(task.name.clone(), 0);
        adj.insert(task.name.clone(), Vec::new());
    }
    for task in tasks {
        for dep in &task.deps {
            if !indegree.contains_key(dep) { return Err(format!("Unknown dependency: {}", dep)); }
            if let Some(list) = adj.get_mut(dep) { list.push(task.name.clone()); }
            if let Some(count) = indegree.get_mut(&task.name) { *count += 1; }
        }
    }
    let mut queue = VecDeque::new();
    for (name, &count) in &indegree { if count == 0 { queue.push_back(name.clone()); } }
    let mut order = Vec::new();
    while let Some(u) = queue.pop_front() {
        order.push(u.clone());
        if let Some(neighbors) = adj.get(&u) {
            for v in neighbors {
                if let Some(count) = indegree.get_mut(v) {
                    *count -= 1;
                    if *count == 0 { queue.push_back(v.clone()); }
                }
            }
        }
    }
    if order.len() == tasks.len() { Ok(order) } else { Err("Cycle detected or unresolved dependency".to_string()) }
}