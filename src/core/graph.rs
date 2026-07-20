#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::time::Duration;
use serde::{Serialize, Deserialize};

// 修正点: 具象クラスではなく、抽象化トレイトをインポート
use crate::core::executor::Executor;
use crate::core::path::PathStrategy;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub name: String,
    pub command: String,
    pub deps: Vec<String>,
    pub timeout_secs: u64,
    pub max_retries: usize,
}

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

    // 修正点: 具象型から Arc<dyn Executor> トレイトオブジェクトへの変更
    pub async fn run(&mut self, executor: Arc<dyn Executor>) {
        let completed = Arc::new(Mutex::new(HashSet::new()));
        let running = Arc::new(Mutex::new(HashSet::new()));
        
        let notify_loop_event = Arc::new(Notify::new());
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
                notify_loop_event.notified().await;
                continue;
            }

            for task_name in ready_tasks {
                let task = self.tasks.get(&task_name).unwrap().clone();
                let executor_clone = Arc::clone(&executor);
                let completed_clone = Arc::clone(&completed);
                let running_clone = Arc::clone(&running);
                let notify_clone = Arc::clone(&notify_loop_event);

                // 実行中フラグを先に立てる
                running.lock().await.insert(task_name.clone());

                tokio::spawn(async move {
                    let mut retry_count = 0;
                    let mut success = false;

                    loop {
                        println!(
                            "🎯 [DagScheduler] パス計算・投入 [試行 {}/{}]: タスク '{}'",
                            retry_count + 1, task.max_retries, task.name
                        );

                        // トレイトのsubmitメソッド経由で、内部のパス選択から実行・解放までを安全に委ねる
                        match executor_clone.submit(task.clone(), PathStrategy::Fastest).await {
                            Ok(_) => {
                                success = true;
                                break;
                            }
                            Err(e) => {
                                eprintln!("⚠️ [DagScheduler] 実行失敗 [タスク: {}]: {}", task.name, e);

                                retry_count += 1;
                                if retry_count >= task.max_retries {
                                    eprintln!("❌ [DagScheduler] タスク '{}' は最大再送回数に達したため失敗しました。", task.name);
                                    break;
                                }

                                println!("🔄 [DagScheduler] 代替経路によるリトライまで待機中...");
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }

                    let mut r_guard = running_clone.lock().await;
                    r_guard.remove(&task.name);
                    
                    if success {
                        let mut c_guard = completed_clone.lock().await;
                        c_guard.insert(task.name.clone());
                        println!("✅ [DagScheduler] タスク '{}' 完了", task.name);
                    } else {
                        println!("💀 [DagScheduler] タスク '{}' 救済不能", task.name);
                    }
                    
                    notify_clone.notify_one();
                });
            }
        }
    }
}