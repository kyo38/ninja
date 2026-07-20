#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::time::Duration;
use serde::{Serialize, Deserialize};

use crate::core::executor::{Executor, TaskResult};
use crate::core::worker::WorkerRegistry;
use crate::core::path::PathStrategy;
use crate::core::retry::RetryPolicy;

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

    pub async fn run(
        &mut self, 
        executor: Arc<dyn Executor>, 
        registry: WorkerRegistry,
        strategy: Arc<dyn PathStrategy>,
        retry_policy: Arc<dyn RetryPolicy>,
    ) {
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
                let registry_clone = registry.clone();
                let strategy_clone = Arc::clone(&strategy);
                let retry_policy_clone = Arc::clone(&retry_policy);
                let completed_clone = Arc::clone(&completed);
                let running_clone = Arc::clone(&running);
                let notify_clone = Arc::clone(&notify_loop_event);

                running.lock().await.insert(task_name.clone());

                tokio::spawn(async move {
                    let mut consumed_retries = 0;   
                    let mut continuous_failures = 0; 
                    let mut success = false;
                    
                    // 💡 初期値の None への代入をやめ、型の宣言だけにすることで
                    //    「一度も読まれずに上書きされた」という警告を根本から防ぎます。
                    let mut last_result: Option<TaskResult>;

                    loop {
                        let target_worker_res = {
                            let workers = registry_clone.get_cloned_sessions().await;
                            strategy_clone.select_path(&workers)
                        };

                        let target_address = match target_worker_res {
                            Ok(addr) => addr,
                            Err(e) => {
                                eprintln!("⚠️ [DagScheduler] パス選択失敗 [タスク: {}]: {}", task.name, e);
                                let mock_res = TaskResult::InfraError { 
                                    node: "unknown".to_string(), 
                                    reason: format!("利用可能なパスがありません: {}", e) 
                                };
                                
                                let res_to_check = mock_res.clone();
                                last_result = Some(mock_res);

                                if !retry_policy_clone.should_retry(&res_to_check, consumed_retries, task.max_retries) {
                                    break;
                                }
                                continuous_failures += 1;
                                tokio::time::sleep(retry_policy_clone.backoff(continuous_failures)).await;
                                continue;
                            }
                        };

                        println!(
                            "🎯 [DagScheduler] パス決定 -> ターゲット: {} [タスク消費リトライ: {}/{}]: タスク '{}'",
                            target_address, consumed_retries, task.max_retries, task.name
                        );

                        let timeout_duration = Duration::from_secs(task.timeout_secs);
                        let exec_future = executor_clone.submit(task.clone(), target_address.clone());

                        let current_res = match tokio::time::timeout(timeout_duration, exec_future).await {
                            Ok(Ok(task_result)) => task_result,
                            Ok(Err(e)) => TaskResult::InfraError {
                                node: target_address.clone(),
                                reason: format!("システム内部実行エラー: {}", e),
                            },
                            Err(_) => TaskResult::Timeout,
                        };

                        let res_to_check = current_res.clone();
                        last_result = Some(current_res);

                        match &res_to_check {
                            TaskResult::Success(msg) => {
                                println!("✅ [DagScheduler] タスク '{}' 成功応答受信: {}", task.name, msg);
                                success = true;
                                break;
                            }
                            TaskResult::InfraError { node, reason } => {
                                eprintln!("⚙️ [DagScheduler] インフラ障害を検知 [ノード: {}]: {}. リトライ上限は消費せず別ルート迂回を試みます。", node, reason);
                            }
                            TaskResult::TaskFailed { reason } => {
                                eprintln!("⚠️ [DagScheduler] タスク自体が失敗コードを返しました: {}", reason);
                                consumed_retries += 1;
                            }
                            TaskResult::Timeout => {
                                eprintln!("⏱️ [DagScheduler] タイムアウトを検出しました（{}秒超過）", task.timeout_secs);
                                consumed_retries += 1;
                            }
                        }

                        if !retry_policy_clone.should_retry(&res_to_check, consumed_retries, task.max_retries) {
                            break;
                        }

                        continuous_failures += 1;
                        let next_backoff = retry_policy_clone.backoff(continuous_failures);
                        println!("🔄 [DagScheduler] 次のリトライ・迂回まで待機中... (待機時間: {:?})", next_backoff);
                        tokio::time::sleep(next_backoff).await;
                    }

                    let mut r_guard = running_clone.lock().await;
                    r_guard.remove(&task.name);
                    
                    if success {
                        let mut c_guard = completed_clone.lock().await;
                        c_guard.insert(task.name.clone());
                        println!("🏁 [DagScheduler] タスク '{}' 完了処理を確定", task.name);
                    } else {
                        println!("💀 [DagScheduler] タスク '{}' 救済不能として確定。最終ステータス: {:?}", task.name, last_result);
                    }
                    
                    notify_clone.notify_one();
                });
            }
        }
    }
}