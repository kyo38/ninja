use crate::protocol::TaskSpec;
use std::collections::HashMap;
use thiserror::Error;

pub use crate::protocol::{TaskResultSpec as TaskResult, TaskSpec as Task};

#[derive(Debug, Error)]
pub enum DagError {
    #[error("タスクが見つかりません: {0}")]
    TaskNotFound(String),

    #[error("循環依存が検出されました (DAG違反)")]
    CyclicDependency,

    #[error("無効なタスク状態遷移です: {0}")]
    InvalidStatusTransition(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
pub struct TaskNode {
    pub spec: TaskSpec,
    pub status: TaskStatus,
    pub children: Vec<String>,
    pub in_degree: usize,
}

#[derive(Debug, Clone, Default)]
pub struct Dag {
    nodes: HashMap<String, TaskNode>,
}

impl Dag {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    /// タスクを追加し、spec 内に記載された dependencies を元に入次数を構築
    pub fn add_task(&mut self, spec: TaskSpec) {
        let id = spec.task_id.clone();
        let deps = spec.dependencies.clone();

        self.nodes.entry(id.clone()).or_insert(TaskNode {
            spec,
            status: TaskStatus::Pending,
            children: Vec::new(),
            in_degree: 0,
        });

        // 既知の依存関係があればエッジを張る
        for dep_id in deps {
            let _ = self.add_dependency(&dep_id, &id);
        }
    }

    pub fn add_dependency(&mut self, parent_id: &str, child_id: &str) -> Result<(), DagError> {
        if !self.nodes.contains_key(parent_id) || !self.nodes.contains_key(child_id) {
            // ノードが揃っていない段階での呼び出しはスキップを許可、またはエラー処理
            return Ok(());
        }

        if let Some(parent_node) = self.nodes.get_mut(parent_id) {
            if !parent_node.children.contains(&child_id.to_string()) {
                parent_node.children.push(child_id.to_string());
            }
        }

        if let Some(child_node) = self.nodes.get_mut(child_id) {
            child_node.in_degree += 1;
        }

        Ok(())
    }

    pub fn get_ready_tasks(&self) -> Vec<TaskSpec> {
        self.nodes
            .values()
            .filter(|node| node.status == TaskStatus::Pending && node.in_degree == 0)
            .map(|node| node.spec.clone())
            .collect()
    }

    pub fn mark_running(&mut self, task_id: &str) -> Result<(), DagError> {
        let node = self
            .nodes
            .get_mut(task_id)
            .ok_or_else(|| DagError::TaskNotFound(task_id.to_string()))?;

        if node.status != TaskStatus::Pending {
            return Err(DagError::InvalidStatusTransition(format!(
                "Task {} is not Pending",
                task_id
            )));
        }

        node.status = TaskStatus::Running;
        Ok(())
    }

    pub fn mark_completed(&mut self, task_id: &str) -> Result<Vec<TaskSpec>, DagError> {
        let children = {
            let node = self
                .nodes
                .get_mut(task_id)
                .ok_or_else(|| DagError::TaskNotFound(task_id.to_string()))?;

            node.status = TaskStatus::Completed;
            node.children.clone()
        };

        let mut newly_ready = Vec::new();

        for child_id in children {
            if let Some(child_node) = self.nodes.get_mut(&child_id) {
                if child_node.in_degree > 0 {
                    child_node.in_degree -= 1;
                }
                if child_node.in_degree == 0 && child_node.status == TaskStatus::Pending {
                    newly_ready.push(child_node.spec.clone());
                }
            }
        }

        Ok(newly_ready)
    }

    pub fn mark_failed(&mut self, task_id: &str) -> Result<(), DagError> {
        let node = self
            .nodes
            .get_mut(task_id)
            .ok_or_else(|| DagError::TaskNotFound(task_id.to_string()))?;

        node.status = TaskStatus::Failed;
        Ok(())
    }

    pub fn is_finished(&self) -> bool {
        self.nodes
            .values()
            .all(|node| node.status == TaskStatus::Completed || node.status == TaskStatus::Failed)
    }

    pub fn validate_dag(&self) -> Result<(), DagError> {
        let mut in_degrees: HashMap<String, usize> = self
            .nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.in_degree))
            .collect();

        let mut zero_in_degree: Vec<String> = in_degrees
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(id, _)| id.clone())
            .collect();

        let mut visited_count = 0;

        while let Some(id) = zero_in_degree.pop() {
            visited_count += 1;
            if let Some(node) = self.nodes.get(&id) {
                for child_id in &node.children {
                    if let Some(deg) = in_degrees.get_mut(child_id) {
                        *deg -= 1;
                        if *deg == 0 {
                            zero_in_degree.push(child_id.clone());
                        }
                    }
                }
            }
        }

        if visited_count == self.nodes.len() {
            Ok(())
        } else {
            Err(DagError::CyclicDependency)
        }
    }
}