// src/core/graph.rs

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// タスクの実行結果
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskResult {
    /// 正常終了（Exit Code 0）
    Success { stdout: String },
    /// コマンド自体の実行失敗（Exit Code 1など - アプリケーション論理エラー）
    Failure { exit_code: i32, stderr: String },
    /// インフラ障害（Worker応答なし、タイムアウト、通信途絶）
    InfraError { reason: String },
}

/// タスクの詳細なライフサイクル状態
#[derive(Debug, Clone, PartialEq)]
pub enum TaskState {
    /// 待機中
    Pending,
    /// 実行中（割り当てられた Worker ID）
    Running { worker_id: usize },
    /// リトライ待機中（現在の試行回数）
    Retrying { attempt: u32 },
    /// 成功完了
    Success,
    /// 最終失敗（リトライ上限超過等）
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub name: String,
    pub command: String,
    pub dependencies: Vec<String>,
    pub timeout_secs: u64,
    pub max_retries: u32,
}

pub struct DagScheduler {
    tasks: Vec<Task>,
    adj_list: HashMap<String, Vec<String>>,
    in_degree: HashMap<String, usize>,
}

impl DagScheduler {
    pub fn new(tasks: Vec<Task>) -> Result<Self, String> {
        let mut adj_list: HashMap<String, Vec<String>> = HashMap::new();
        let mut in_degree: HashMap<String, usize> = HashMap::new();

        for task in &tasks {
            adj_list.entry(task.name.clone()).or_default();
            in_degree.entry(task.name.clone()).or_insert(0);
        }

        for task in &tasks {
            for dep in &task.dependencies {
                if !in_degree.contains_key(dep) {
                    return Err(format!("存在しない依存タスクです: {}", dep));
                }
                adj_list.entry(dep.clone()).or_default().push(task.name.clone());
                *in_degree.entry(task.name.clone()).or_insert(0) += 1;
            }
        }

        let sched = Self { tasks, adj_list, in_degree };
        if sched.has_cycle() {
            return Err("DAGに循環依存（サイクル）が検出されました".to_string());
        }

        Ok(sched)
    }

    fn has_cycle(&self) -> bool {
        let mut in_degree_copy = self.in_degree.clone();
        let mut queue: Vec<String> = in_degree_copy
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(name, _)| name.clone())
            .collect();

        let mut visited_count = 0;

        while let Some(node) = queue.pop() {
            visited_count += 1;
            if let Some(neighbors) = self.adj_list.get(&node) {
                for neighbor in neighbors {
                    if let Some(deg) = in_degree_copy.get_mut(neighbor) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push(neighbor.clone());
                        }
                    }
                }
            }
        }

        visited_count != self.tasks.len()
    }

    /// 現在実行可能なタスクのリストを返す
    pub fn get_ready_tasks(&self, states: &HashMap<String, TaskState>) -> Vec<String> {
        let mut ready = Vec::new();

        for task in &self.tasks {
            if let Some(TaskState::Pending) = states.get(&task.name) {
                let deps_satisfied = task.dependencies.iter().all(|dep| {
                    matches!(states.get(dep), Some(TaskState::Success))
                });

                if deps_satisfied {
                    ready.push(task.name.clone());
                }
            }
        }

        ready
    }
}