#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::time::{sleep, Duration};
use std::net::TcpStream;
use serde::{Serialize, Deserialize};

/// DAG内の個々のタスクを表す構造体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub name: String,
    pub command: String,
    pub deps: Vec<String>,
    pub timeout_secs: u64,
    pub max_retries: usize,
}

/// ワーカーノードの状態を管理する構造体
#[derive(Debug, Clone)]
pub struct WorkerSlot {
    pub addr: String,
    pub is_alive: bool,
    pub is_busy: bool,
}

/// ネットワーク上の複数ワーカーの調停と、リクエストの割り当てを制御する
pub struct RemoteExecutor {
    pub workers: Arc<Mutex<Vec<WorkerSlot>>>,
    pub notify_free_worker: Arc<Notify>,
}

impl RemoteExecutor {
    pub fn new(addresses: Vec<String>) -> Self {
        let slots = addresses
            .into_iter()
            .map(|addr| WorkerSlot {
                addr,
                is_alive: true,
                is_busy: false,
            })
            .collect();

        Self {
            workers: Arc::new(Mutex::new(slots)),
            notify_free_worker: Arc::new(Notify::new()),
        }
    }

    pub async fn acquire_worker(&self) -> WorkerSlot {
        loop {
            let mut slots = self.workers.lock().await;
            let found_slot = slots.iter_mut().find(|slot| slot.is_alive && !slot.is_busy);

            if let Some(slot) = found_slot {
                slot.is_busy = true;
                return slot.clone();
            }

            drop(slots); 
            self.notify_free_worker.notified().await;
        }
    }

    pub async fn release_worker(&self, addr: String) {
        let mut slots = self.workers.lock().await;
        if let Some(slot) = slots.iter_mut().find(|s| s.addr == addr) {
            slot.is_busy = false;
            println!("🔓 [RemoteExecutor] ワーカー ( {} ) が解放されました。", addr);
            self.notify_free_worker.notify_one();
        }
    }

    pub async fn update_node_quality(&self, addr: &str, is_alive: bool) {
        let mut slots = self.workers.lock().await;
        if let Some(slot) = slots.iter_mut().find(|s| s.addr == addr) {
            if slot.is_alive != is_alive {
                slot.is_alive = is_alive;
                if is_alive {
                    println!("💖 [RemoteExecutor] ワーカー ( {} ) の復帰を確認しました。", addr);
                } else {
                    println!("🔴 [RemoteExecutor] ワーカー ( {} ) の品質低下（ダウン）を検知しました。", addr);
                }
                self.notify_free_worker.notify_waiters();
            }
        }
    }

    pub async fn start_heartbeat_loop(&self, interval: Duration, timeout: Duration) {
        let workers_clone = Arc::clone(&self.workers);
        let notify_clone = Arc::clone(&self.notify_free_worker);

        tokio::spawn(async move {
            loop {
                sleep(interval).await;
                let mut slots = workers_clone.lock().await;

                for slot in slots.iter_mut() {
                    let alive = match TcpStream::connect_timeout(
                        &slot.addr.parse().unwrap(),
                        timeout,
                    ) {
                        Ok(_) => true,
                        Err(_) => false,
                    };

                    if slot.is_alive != alive {
                        slot.is_alive = alive;
                        if alive {
                            println!("💖 [Heartbeat] ワーカーノード ( {} ) が復帰しました。", slot.addr);
                        } else {
                            println!("🔴 [Heartbeat] ワーカーノード ( {} ) の切断・ハングアップを検知しました！", slot.addr);
                        }
                        notify_clone.notify_waiters();
                    }
                }
            }
        });
    }
}

/// DAG全体の依存関係を解決しながらタスク実行をコントロールするスケジューラ
pub struct DagScheduler {
    pub tasks: HashMap<String, Task>,
    pub adjacency_list: HashMap<String, Vec<String>>,
    pub in_degree: HashMap<String, usize>,
}

impl DagScheduler {
    pub fn new(tasks: Vec<Task>) -> Result<Self, String> {
        let mut adj = HashMap::new();
        let mut in_deg = HashMap::new();
        let mut task_map = HashMap::new();

        for task in &tasks {
            adj.insert(task.name.clone(), Vec::new());
            in_deg.insert(task.name.clone(), 0);
            task_map.insert(task.name.clone(), task.clone());
        }

        for task in &tasks {
            for dep in &task.deps {
                if !task_map.contains_key(dep) {
                    return Err(format!("タスク '{}' が依存している '{}' が見つかりません。", task.name, dep));
                }
                adj.entry(dep.clone()).or_insert_with(Vec::new).push(task.name.clone());
                *in_deg.entry(task.name.clone()).or_insert(0) += 1;
            }
        }

        Ok(Self {
            tasks: task_map,
            adjacency_list: adj,
            in_degree: in_deg,
        })
    }

    pub fn get_ready_tasks(&self, completed: &HashSet<String>, running: &HashSet<String>) -> Vec<String> {
        let mut ready = Vec::new();
        for (task_id, &_deg) in &self.in_degree {
            if completed.contains(task_id) || running.contains(task_id) {
                continue;
            }
            let deps = &self.tasks[task_id].deps;
            if deps.iter().all(|d| completed.contains(d)) {
                ready.push(task_id.clone());
            }
        }
        ready
    }

    pub async fn run(&mut self, executor: Arc<RemoteExecutor>) {
        let completed = Arc::new(Mutex::new(HashSet::new()));
        let running = Arc::new(Mutex::new(HashSet::new()));
        let notify_task_complete = Arc::new(Notify::new());
        let total_tasks = self.tasks.len();

        println!("🚀 [DagScheduler] DAGの実行を開始します。総タスク数: {}", total_tasks);

        loop {
            let completed_guard = completed.lock().await;
            let running_guard = running.lock().await;

            if completed_guard.len() == total_tasks {
                println!("🏁 [DagScheduler] すべてのタスクが正常に完了しました！");
                break;
            }

            let ready_tasks = self.get_ready_tasks(&completed_guard, &running_guard);

            drop(completed_guard);
            drop(running_guard);

            if ready_tasks.is_empty() {
                notify_task_complete.notified().await;
                continue;
            }

            for task_name in ready_tasks {
                let task = self.tasks.get(&task_name).unwrap().clone();
                let executor_clone = Arc::clone(&executor);
                let completed_clone = Arc::clone(&completed);
                let running_clone = Arc::clone(&running);
                let notify_clone = Arc::clone(&notify_task_complete);

                running.lock().await.insert(task_name.clone());

                tokio::spawn(async move {
                    println!("⏳ [DagScheduler] タスク '{}' のワーカーを確保中...", task.name);
                    let worker = executor_clone.acquire_worker().await;
                    println!("🎯 [DagScheduler] ワーカー ( {} ) を確保。タスク '{}' を割り当てます。", worker.addr, task.command);

                    // --- [擬似的なタスク実行処理] ---
                    tokio::time::sleep(Duration::from_secs(2)).await; 
                    // ---------------------------------

                    executor_clone.release_worker(worker.addr).await;

                    let mut r_guard = running_clone.lock().await;
                    r_guard.remove(&task.name);
                    
                    let mut c_guard = completed_clone.lock().await;
                    c_guard.insert(task.name.clone());

                    println!("✅ [DagScheduler] タスク '{}' が完了しました。", task.name);
                    notify_clone.notify_one();
                });
            }
        }
    }
}