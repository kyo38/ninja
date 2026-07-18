// src/core/graph.rs

use serde::{Serialize, Deserialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc;

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

/// 📦 アクター（メインループ）に送るメッセージの定義
enum Event {
    /// タスクが完了した通知 (タスク名, 成否)
    TaskFinished(String, bool),
}

/// 🛡️ Mutex / RwLock を完全に排除した、チャネル駆動型 DagScheduler
pub struct DagScheduler {
    tasks: HashMap<String, Task>,
    adjacency_list: HashMap<String, Vec<String>>,
    
    // 初期状態の入次数とステータス（不変データとして保持）
    initial_indegrees: HashMap<String, usize>,
    initial_statuses: HashMap<String, TaskStatus>,
    
    // 実行時にReadyになったタスクを投入する初期キュー
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
                // 実際に初期状態で依存ゼロのタスクを収集
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

    /// 🔄 状態をローカルに閉じ込めた、完全メッセージ駆動のメインループ
    pub async fn run(self, executor: Arc<dyn Executor>) {
        println!("🚀 [Ninja Engine] --- ロックフリー・チャネル駆動タスクループ開始 ---");

        // 📝 状態（State）はすべてこの関数内のローカル変数として管理。ロックは一切不要
        let mut indegrees = self.initial_indegrees;
        let mut statuses = self.initial_statuses;
        let mut running_count = 0;
        let mut has_failed = false;

        // タスク完了イベントを集約するメインチャネル
        let (event_tx, mut event_rx) = mpsc::channel::<Event>(1024);
        
        // 実行可能（Ready）になったタスクを管理する内部キュー
        let mut ready_queue = VecDeque::from(self.initial_ready_tasks);

        loop {
            // 1. エラーが発生していなければ、Readyなタスクを可能な限りすべてSpawnする
            if !has_failed {
                while let Some(task_name) = ready_queue.pop_front() {
                    let task = self.tasks.get(&task_name).unwrap().clone();
                    let event_tx_clone = event_tx.clone();
                    let exec_clone = Arc::clone(&executor);

                    statuses.insert(task_name.clone(), TaskStatus::Running);
                    running_count += 1;

                    // バックグラウンドで非同期実行。終わったらチャネルにイベントを投げるだけ
                    tokio::spawn(async move {
                        let success = exec_clone.execute(&task).await;
                        let _ = event_tx_clone.send(Event::TaskFinished(task.name, success)).await;
                    });
                }
            }

            // 2. 稼働中のタスクがなく、Readyキューも空なら全グラフ終了
            if running_count == 0 && ready_queue.is_empty() {
                break;
            }

            // 3. メッセージ待ち受信用アクターコア
            // ロックを一切取らず、チャネルから流れてくるイベントのみで状態を安全に更新
            if let Some(event) = event_rx.recv().await {
                match event {
                    Event::TaskFinished(finished_task, success) => {
                        running_count -= 1;

                        if !success {
                            has_failed = true;
                            statuses.insert(finished_task.clone(), TaskStatus::Failed);
                            println!("❌ [Ninja Engine] タスク [{}] が失敗しました。後続の発火を停止します。", finished_task);
                            continue;
                        }

                        statuses.insert(finished_task.clone(), TaskStatus::Done);

                        // 後続タスクの依存度（入次数）を下げる
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

        // 結果報告
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