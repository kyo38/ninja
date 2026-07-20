#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, Instant};
use serde::{Serialize, Deserialize};

use crate::core::executor::{Executor, TaskResult};
use crate::core::worker::WorkerRegistry;
use crate::core::path::PathStrategy;
use crate::core::retry::RetryPolicy;

/// 🏅 Taskの状態遷移を明示化するエニュメレーション
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    Pending,
    Running,
    Retrying,
    Success,
    Failed,
}

/// 🏅 分散デバッグのためのトレースIDを内蔵するコンテキスト
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskContext {
    pub trace_id: String,
}

impl TaskContext {
    /// 簡易的なユニークトレースIDを生成
    pub fn new_random(task_name: &str) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        task_name.hash(&mut hasher);
        Instant::now().hash(&mut hasher);
        let id = hasher.finish();

        Self {
            trace_id: format!("tr-{:08x}", id),
        }
    }
}

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

    /// 状態マップを元に、実行可能なタスクを抽出
    pub fn get_ready_tasks(&self, state_map: &HashMap<String, TaskState>) -> Vec<String> {
        let mut ready = Vec::new();
        for task_id in self.in_degree.keys() {
            if let Some(&state) = state_map.get(task_id) {
                if state != TaskState::Pending {
                    continue;
                }
            }

            let deps = &self.tasks[task_id].deps;
            let all_deps_success = deps.iter().all(|d| {
                state_map.get(d) == Some(&TaskState::Success)
            });

            if all_deps_success {
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
        let state_map = Arc::new(Mutex::new(HashMap::<String, TaskState>::new()));
        
        {
            let mut states = state_map.lock().await;
            for task_name in self.tasks.keys() {
                states.insert(task_name.clone(), TaskState::Pending);
            }
        }

        let notify_loop_event = Arc::new(Notify::new());
        let total_tasks = self.tasks.len();

        println!("🚀 [DagScheduler] DAGの実行を開始します。総タスク数: {}", total_tasks);

        loop {
            let states_guard = state_map.lock().await;

            let finished_count = states_guard.values().filter(|&&s| s == TaskState::Success || s == TaskState::Failed).count();
            if finished_count == total_tasks {
                let success_count = states_guard.values().filter(|&&s| s == TaskState::Success).count();
                println!("🏁 [DagScheduler] 全タスクの処理が確定しました。[成功: {}/{}]", success_count, total_tasks);
                break;
            }

            let ready_tasks = self.get_ready_tasks(&states_guard);

            drop(states_guard);

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
                let state_map_clone = Arc::clone(&state_map);
                let notify_clone = Arc::clone(&notify_loop_event);

                state_map.lock().await.insert(task_name.clone(), TaskState::Running);

                tokio::spawn(async move {
                    let context = TaskContext::new_random(&task.name);
                    println!("[{}] 🟢 タスク '{}' の状態遷移: Pending -> Running", context.trace_id, task.name);

                    let mut consumed_retries = 0;   
                    let mut continuous_failures = 0; 
                    let mut success = false;
                    
                    let mut last_result: Option<TaskResult>;

                    loop {
                        let target_worker_res = {
                            let workers = registry_clone.get_cloned_sessions().await;
                            strategy_clone.select_path(&workers)
                        };

                        let target_address = match target_worker_res {
                            Ok(addr) => addr,
                            Err(e) => {
                                eprintln!("[{}] ⚠️ パス選択失敗 [タスク: {}]: {}", context.trace_id, task.name, e);
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
                                
                                state_map_clone.lock().await.insert(task.name.clone(), TaskState::Retrying);
                                println!("[{}] 🔄 状態遷移: Running/Retrying -> Retrying (パス枯渇待機)", context.trace_id);
                                
                                tokio::time::sleep(retry_policy_clone.backoff(continuous_failures)).await;
                                continue;
                            }
                        };

                        println!(
                            "[{}] 🎯 パス決定 -> ターゲット: {} [タスク消費リトライ: {}/{}]: タスク '{}'",
                            context.trace_id, target_address, consumed_retries, task.max_retries, task.name
                        );

                        let start_time = Instant::now();
                        let timeout_duration = Duration::from_secs(task.timeout_secs);
                        let exec_future = executor_clone.submit(task.clone(), target_address.clone());

                        let raw_res = match tokio::time::timeout(timeout_duration, exec_future).await {
                            Ok(Ok(task_result)) => task_result,
                            Ok(Err(e)) => TaskResult::InfraError {
                                node: target_address.clone(),
                                reason: format!("システム内部実行エラー: {}", e),
                            },
                            Err(_) => TaskResult::Timeout {
                                worker: target_address.clone(),
                                duration: start_time.elapsed(),
                            },
                        };

                        let elapsed_duration = start_time.elapsed();
                        let current_res = match raw_res {
                            TaskResult::Success { message, worker, .. } => TaskResult::Success {
                                worker,
                                duration: elapsed_duration,
                                attempt: consumed_retries + 1,
                                message,
                            },
                            TaskResult::TaskFailed { reason, worker, .. } => TaskResult::TaskFailed {
                                worker,
                                duration: elapsed_duration,
                                attempt: consumed_retries + 1,
                                reason,
                            },
                            other => other,
                        };

                        let res_to_check = current_res.clone();
                        last_result = Some(current_res);

                        match &res_to_check {
                            TaskResult::Success { worker, duration, attempt, message } => {
                                println!(
                                    "[{}] ✅ タスク '{}' 成功応答受信 [Worker: {}, 試行回数: {}, 処理時間: {:?}]: {}", 
                                    context.trace_id, task.name, worker, attempt, duration, message
                                );
                                success = true;
                                break;
                            }
                            TaskResult::InfraError { node, reason } => {
                                eprintln!("[{}] ⚙️ インフラ障害検知 [ノード: {}]: {}. 別ルートを模索します。", context.trace_id, node, reason);
                            }
                            TaskResult::TaskFailed { worker, duration, attempt, reason } => {
                                eprintln!(
                                    "[{}] ⚠️ ワーカー側タスク失敗 [Worker: {}, 試行回数: {}, 処理時間: {:?}]: {}", 
                                    context.trace_id, worker, attempt, duration, reason
                                );
                                consumed_retries += 1;
                            }
                            TaskResult::Timeout { worker, duration } => {
                                eprintln!("[{}] ⏱️ タイムアウト検出 [Worker: {}, 処理時間: {:?}（{}秒制限）]", context.trace_id, worker, duration, task.timeout_secs);
                                consumed_retries += 1;
                            }
                        }

                        if !retry_policy_clone.should_retry(&res_to_check, consumed_retries, task.max_retries) {
                            break;
                        }

                        continuous_failures += 1;
                        let next_backoff = retry_policy_clone.backoff(continuous_failures);
                        
                        state_map_clone.lock().await.insert(task.name.clone(), TaskState::Retrying);
                        println!("[{}] 🔄 状態遷移 -> Retrying. 次の試行まで待機: {:?}", context.trace_id, next_backoff);
                        
                        tokio::time::sleep(next_backoff).await;
                    }
                    
                    let mut states = state_map_clone.lock().await;
                    if success {
                        states.insert(task.name.clone(), TaskState::Success);
                        println!("[{}] 🏁 状態確定: Success 🏆 (タスク: '{}')", context.trace_id, task.name);
                    } else {
                        states.insert(task.name.clone(), TaskState::Failed);
                        println!("[{}] 💀 状態確定: Failed ❌ (タスク: '{}', 最終結果メトリクス: {:?})", context.trace_id, task.name, last_result);
                    }
                    
                    notify_clone.notify_one();
                });
            }
        }
    }
}

// =========================================================================
// 🧪 ユニットテスト領域
// =========================================================================
#[cfg(test)]
mod tests {
    use super::*;

    fn create_dummy_task(name: &str, deps: Vec<&str>) -> Task {
        Task {
            name: name.to_string(),
            command: "echo test".to_string(),
            deps: deps.into_iter().map(|s| s.to_string()).collect(),
            timeout_secs: 10,
            max_retries: 3,
        }
    }

    #[test]
    fn test_dag_initialization_success() {
        let t1 = create_dummy_task("task1", vec![]);
        let t2 = create_dummy_task("task2", vec!["task1"]); // t2はt1に依存

        let scheduler_res = DagScheduler::new(vec![t1, t2]);
        assert!(scheduler_res.is_ok());

        let scheduler = scheduler_res.unwrap();
        // 入次数（in-degree）の検証
        assert_eq!(*scheduler.in_degree.get("task1").unwrap(), 0);
        assert_eq!(*scheduler.in_degree.get("task2").unwrap(), 1);
    }

    #[test]
    fn test_dag_initialization_missing_dependency() {
        let t1 = create_dummy_task("task1", vec!["ghost_task"]); // 存在しないタスクに依存

        let scheduler_res = DagScheduler::new(vec![t1]);
        assert!(scheduler_res.is_err());
        let err_msg = scheduler_res.err().unwrap();
        assert!(err_msg.contains("見つかりません"));
    }

    #[test]
    fn test_get_ready_tasks_filtration() {
        let t1 = create_dummy_task("task1", vec![]);
        let t2 = create_dummy_task("task2", vec!["task1"]);
        let t3 = create_dummy_task("task3", vec![]);

        let scheduler = DagScheduler::new(vec![t1, t2, t3]).unwrap();
        
        let mut state_map = HashMap::new();
        state_map.insert("task1".to_string(), TaskState::Pending);
        state_map.insert("task2".to_string(), TaskState::Pending);
        state_map.insert("task3".to_string(), TaskState::Pending);

        // 初期状態：依存のない task1 と task3 だけが実行可能であること
        let mut ready = scheduler.get_ready_tasks(&state_map);
        ready.sort();
        assert_eq!(ready, vec!["task1".to_string(), "task3".to_string()]);

        // task1 が正常完了（Success）し、task3 が実行中（Running）になったと仮定
        state_map.insert("task1".to_string(), TaskState::Success);
        state_map.insert("task3".to_string(), TaskState::Running);

        // 次の判定：依存が解消された task2 のみが抽出されること（Runningのtask3は除外）
        let ready_next = scheduler.get_ready_tasks(&state_map);
        assert_eq!(ready_next, vec!["task2".to_string()]);
    }
}