// src/core/graph.rs

use serde::{Serialize, Deserialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, mpsc};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub name: String,
    pub deps: Vec<String>,
    pub command: String,
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

/// 🌐 非同期対応したExecutorトレイト
#[async_trait::async_trait]
pub trait Executor: Send + Sync {
    async fn execute(&self, task: &Task) -> bool;
}

pub struct LocalExecutor;

#[async_trait::async_trait]
impl Executor for LocalExecutor {
    async fn execute(&self, task: &Task) -> bool {
        println!("  ⚡ [LocalExecute] ➔ [{}] Running: {}", task.name, task.command);
        tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;
        true
    }
}

/// 🛡️ Tokioの非同期プリミティブで再構築された DagScheduler
pub struct DagScheduler {
    tasks: HashMap<String, Task>,
    adjacency_list: HashMap<String, Vec<String>>,

    indegrees: Mutex<HashMap<String, usize>>,
    running_count: Mutex<usize>,
    total_processed: Mutex<usize>,
    has_failed: Mutex<bool>,
    statuses: RwLock<HashMap<String, TaskStatus>>,

    ready_tx: mpsc::UnboundedSender<String>,
    ready_rx: Mutex<Option<mpsc::UnboundedReceiver<String>>>,
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

        let mut temp_indegrees = base_indegrees.clone();
        let mut validation_queue = VecDeque::new();
        for (name, &deg) in &temp_indegrees {
            if deg == 0 {
                validation_queue.push_back(name.clone());
            }
        }

        let mut sorted_count = 0;
        while let Some(u) = validation_queue.pop_front() {
            sorted_count += 1;
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

        let (ready_tx, ready_rx) = mpsc::unbounded_channel();
        for (name, &deg) in &base_indegrees {
            if deg == 0 {
                ready_tx.send(name.clone()).unwrap();
            }
        }

        Ok(DagScheduler {
            tasks,
            adjacency_list,
            indegrees: Mutex::new(base_indegrees),
            running_count: Mutex::new(0),
            total_processed: Mutex::new(0),
            has_failed: Mutex::new(false),
            statuses: RwLock::new(statuses),
            ready_tx,
            ready_rx: Mutex::new(Some(ready_rx)),
        })
    }

    /// 🔄 完全非同期駆動のメインループ
    pub async fn run(self: Arc<Self>, executor: Arc<dyn Executor>) {
        let (notify_tx, mut notify_rx) = mpsc::channel::<(String, bool)>(1024);
        let mut ready_rx = self.ready_rx.lock().await.take().expect("ready_rx has already been taken");

        println!("🚀 [Ninja Engine] --- 完全非同期タスクループ開始 ---");

        loop {
            let failed = *self.has_failed.lock().await;

            if !failed {
                while let Ok(task_name) = ready_rx.try_recv() {
                    let task = self.tasks.get(&task_name).unwrap().clone();
                    let notify_tx_clone = notify_tx.clone();
                    let exec_clone = Arc::clone(&executor);

                    self.statuses.write().await.insert(task_name.clone(), TaskStatus::Running);
                    *self.running_count.lock().await += 1;

                    tokio::spawn(async move {
                        let success = exec_clone.execute(&task).await;
                        let _ = notify_tx_clone.send((task.name, success)).await;
                    });
                }
            }

            if *self.running_count.lock().await == 0 && ready_rx.is_empty() {
                break;
            }

            if let Some((finished_task, success)) = notify_rx.recv().await {
                *self.running_count.lock().await -= 1;
                *self.total_processed.lock().await += 1;

                if !success {
                    *self.has_failed.lock().await = true;
                    self.statuses.write().await.insert(finished_task.clone(), TaskStatus::Failed);
                    println!("❌ [Ninja Engine] タスク [{}] が失敗しました。後続の発火を停止します。", finished_task);
                    continue;
                }

                self.statuses.write().await.insert(finished_task.clone(), TaskStatus::Done);

                if let Some(followers) = self.adjacency_list.get(&finished_task) {
                    let mut indeg_guard = self.indegrees.lock().await;
                    for follower in followers {
                        if let Some(deg) = indeg_guard.get_mut(follower) {
                            *deg -= 1;
                            if *deg == 0 {
                                self.ready_tx.send(follower.clone()).unwrap();
                            }
                        }
                    }
                }
            }
        }

        self.report_result().await;
    }

    async fn report_result(&self) {
        if *self.has_failed.lock().await {
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