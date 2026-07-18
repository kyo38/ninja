// src/core/graph.rs

use serde::{Serialize, Deserialize};
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

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

pub fn resolve_execution_order(tasks: &[Task]) -> Result<Vec<String>, String> {
    let mut indegree = HashMap::new();
    let mut adj = HashMap::new();
    
    for task in tasks {
        indegree.insert(task.name.clone(), 0);
        adj.insert(task.name.clone(), Vec::new());
    }
    
    for task in tasks {
        for dep in &task.deps {
            if !indegree.contains_key(dep) {
                return Err(format!("Unknown dependency: {}", dep));
            }
            if let Some(list) = adj.get_mut(dep) {
                list.push(task.name.clone());
            }
            if let Some(count) = indegree.get_mut(&task.name) {
                *count += 1;
            }
        }
    }
    
    let mut queue = VecDeque::new();
    for (name, &count) in &indegree {
        if count == 0 {
            queue.push_back(name.clone());
        }
    }
    
    let mut order = Vec::new();
    while let Some(u) = queue.pop_front() {
        order.push(u.clone());
        if let Some(neighbors) = adj.get(&u) {
            for v in neighbors {
                if let Some(count) = indegree.get_mut(v) {
                    *count -= 1;
                    if *count == 0 {
                        queue.push_back(v.clone());
                    }
                }
            }
        }
    }
    
    if order.len() == tasks.len() {
        Ok(order)
    } else {
        Err("Cycle detected or unresolved dependency".to_string())
    }
}

pub trait Executor: Send + Sync {
    fn execute(&self, task: &Task) -> bool;
}

pub struct LocalExecutor;
impl Executor for LocalExecutor {
    fn execute(&self, task: &Task) -> bool {
        println!("  ⚡ [LocalExecute] ➔ [{}] Running: {}", task.name, task.command);
        thread::sleep(std::time::Duration::from_millis(800));
        true
    }
}

pub struct DagScheduler {
    tasks: HashMap<String, Task>,
    indegrees: HashMap<String, usize>,
    adjacency_list: HashMap<String, Vec<String>>,
    statuses: HashMap<String, TaskStatus>,
    ready_queue: VecDeque<String>,
    running_count: usize,
    total_processed: usize,
    has_failed: bool,
}

impl DagScheduler {
    pub fn new(received_tasks: Vec<Task>) -> Self {
        let mut tasks = HashMap::new();
        let mut indegrees = HashMap::new();
        let mut adjacency_list = HashMap::new();
        let mut statuses = HashMap::new();

        for task in received_tasks {
            let name = task.name.clone();
            indegrees.insert(name.clone(), task.deps.len());
            adjacency_list.insert(name.clone(), Vec::new());
            statuses.insert(name.clone(), TaskStatus::Pending);
            tasks.insert(name, task);
        }

        for task in tasks.values() {
            for dep in &task.deps {
                if let Some(list) = adjacency_list.get_mut(dep) {
                    list.push(task.name.clone());
                }
            }
        }

        let mut ready_queue = VecDeque::new();
        for (name, &deg) in &indegrees {
            if deg == 0 {
                ready_queue.push_back(name.clone());
            }
        }

        DagScheduler {
            tasks,
            indegrees,
            adjacency_list,
            statuses,
            ready_queue,
            running_count: 0,
            total_processed: 0,
            has_failed: false,
        }
    }

    pub fn run(&mut self, executor: Arc<dyn Executor>) {
        let (tx, rx) = mpsc::channel::<(String, bool)>();

        println!("🚀 [Ninja Engine] --- スケジューリングループ開始 ---");

        loop {
            if !self.has_failed {
                if self.ready_queue.len() > 1 {
                    let names: Vec<String> = self.ready_queue.iter().cloned().collect();
                    println!("  [⚡ Parallel Ready] 同時並列実行を開始します: {:?}", names);
                }

                while let Some(task_name) = self.ready_queue.pop_front() {
                    let task = self.tasks.get(&task_name).unwrap().clone();
                    let tx_clone = tx.clone();
                    let exec_clone = Arc::clone(&executor);

                    self.statuses.insert(task_name, TaskStatus::Running);
                    self.running_count += 1;

                    thread::spawn(move || {
                        let success = exec_clone.execute(&task);
                        let _ = tx_clone.send((task.name, success));
                    });
                }
            }

            if self.running_count == 0 {
                break;
            }

            if let Ok((finished_task, success)) = rx.recv() {
                self.running_count -= 1;
                self.total_processed += 1;

                if !success {
                    self.has_failed = true;
                    self.statuses.insert(finished_task.clone(), TaskStatus::Failed);
                    println!("❌ [Ninja Engine] タスク [{}] が失敗しました。後続の発火を停止します。", finished_task);
                    continue;
                }

                self.statuses.insert(finished_task.clone(), TaskStatus::Done);

                if let Some(followers) = self.adjacency_list.get(&finished_task) {
                    for follower in followers {
                        if let Some(deg) = self.indegrees.get_mut(follower) {
                            *deg -= 1;
                            if *deg == 0 {
                                self.ready_queue.push_back(follower.clone());
                            }
                        }
                    }
                }
            }
        }

        self.report_result();
    }

    fn report_result(&self) {
        if self.has_failed {
            println!("❌ [Ninja Engine] 一部タスクのエラーにより、実行が中断されました。");
        } else if self.total_processed < self.tasks.len() {
            println!("🛑 [Ninja Engine] 致命的エラー: デッドロックを検出しました。循環依存の可能性があります。");
            let unresolved: Vec<String> = self.indegrees.iter()
                .filter(|&(_, &deg)| deg > 0)
                .map(|(name, _)| name.clone())
                .collect();
            println!("  └── 実行不可能（依存未解消）なタスク群: {:?}", unresolved);
            println!();
        } else {
            println!("🎉 [Ninja Engine] 全てのタスクグラフが依存関係通りに完全実行されました。\n");
        }
    }
}