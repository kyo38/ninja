// src/server/orchestrator.rs

use std::error::Error;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, Notify};

use crate::core::graph::{Task, DagScheduler, TaskState}; // 👈 ninja:: から crate:: へ修正
use super::worker_pool::WorkerPool;
use super::client_handler::ClientHandler;

pub struct Orchestrator {
    worker_addr: String,
    client_addr: String,
    state_map: Arc<Mutex<HashMap<String, TaskState>>>,
    task_lookup: Arc<Mutex<HashMap<String, Task>>>,
    pulse: Arc<Notify>,
}

impl Orchestrator {
    pub fn new(worker_addr: &str, client_addr: &str) -> Self {
        Self {
            worker_addr: worker_addr.to_string(),
            client_addr: client_addr.to_string(),
            state_map: Arc::new(Mutex::new(HashMap::new())),
            task_lookup: Arc::new(Mutex::new(HashMap::new())),
            pulse: Arc::new(Notify::new()),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn Error>> {
        let worker_pool = WorkerPool::new(Arc::clone(&self.pulse));
        worker_pool.start_listener(&self.worker_addr).await?;

        let mut client_handler = ClientHandler::bind(&self.client_addr).await?;

        loop {
            match client_handler.accept_tasks().await {
                Ok(tasks) => {
                    self.execute_dag(tasks, &worker_pool).await?;
                }
                Err(e) => {
                    eprintln!("❌ [Master] Client通信/パースエラー: {:?}", e);
                }
            }
        }
    }

    async fn execute_dag(&self, tasks: Vec<Task>, worker_pool: &WorkerPool) -> Result<(), Box<dyn Error>> {
        {
            let mut s_map = self.state_map.lock().await;
            let mut t_map = self.task_lookup.lock().await;
            s_map.clear();
            t_map.clear();
            for task in tasks.iter() {
                t_map.insert(task.name.clone(), task.clone());
                s_map.insert(task.name.clone(), TaskState::Pending);
            }
        }

        let scheduler = match DagScheduler::new(tasks) {
            Ok(sched) => sched,
            Err(e) => {
                eprintln!("❌ [Master] DAGの初期化に失敗: {:?}", e);
                return Ok(());
            }
        };

        println!("\n--- 🚀 分散 DAG 実行スケジュール開始 ---");

        let workers = worker_pool.get_inner();

        loop {
            let current_states = self.state_map.lock().await.clone();

            let all_finished = current_states.values().all(|s| {
                matches!(s, TaskState::Success | TaskState::Failed)
            });

            if all_finished {
                println!("🎉 全ての分散タスクの実行が完了しました！");
                break;
            }

            let ready_tasks = scheduler.get_ready_tasks(&current_states);
            let mut worker_list = workers.lock().await;

            if !ready_tasks.is_empty() && !worker_list.is_empty() {
                for task_name in ready_tasks {
                    if let Some(mut worker) = worker_list.pop() {
                        let task_lookup_map = self.task_lookup.lock().await;
                        if let Some(task) = task_lookup_map.get(&task_name) {
                            
                            if let Some(state) = self.state_map.lock().await.get_mut(&task_name) {
                                *state = TaskState::Running;
                            }

                            println!("✈️  [Master] Worker {} へタスク [{}] を配信します: {}", worker.id, task.name, task.command);

                            let state_map_inner = Arc::clone(&self.state_map);
                            let workers_inner = Arc::clone(&workers);
                            let pulse_inner = Arc::clone(&self.pulse);
                            let t_name = task.name.clone();
                            let cmd_str = task.command.clone();

                            tokio::spawn(async move {
                                if worker.stream.write_all(cmd_str.as_bytes()).await.is_ok() {
                                    let mut res_buf = vec![0; 1024];
                                    if let Ok(bytes_read) = worker.stream.read(&mut res_buf).await {
                                        let response = String::from_utf8_lossy(&res_buf[..bytes_read]);
                                        let mut s_map = state_map_inner.lock().await;
                                        if let Some(state) = s_map.get_mut(&t_name) {
                                            if response.trim() == "SUCCESS" {
                                                println!("✓ [Master] Worker {} から報告: [{}] 正常終了", worker.id, t_name);
                                                *state = TaskState::Success;
                                            } else {
                                                println!("❌ [Master] Worker {} から報告: [{}] エラー終了", worker.id, t_name);
                                                *state = TaskState::Failed;
                                            }
                                        }
                                    }
                                } else {
                                    println!("⚠️  [Master] Worker {} との通信に失敗。タスク [{}] を保留に戻します", worker.id, t_name);
                                    if let Some(state) = state_map_inner.lock().await.get_mut(&t_name) {
                                        *state = TaskState::Pending;
                                    }
                                }

                                workers_inner.lock().await.push(worker);
                                pulse_inner.notify_waiters();
                            });
                        } else {
                            worker_list.push(worker);
                        }
                    } else {
                        break;
                    }
                }
            }

            drop(worker_list);
            self.pulse.notified().await;
        }

        println!("--- 🏁 分散 DAG 実行スケジュール終了 ---\n");
        Ok(())
    }
}