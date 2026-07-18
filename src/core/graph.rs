// src/core/graph.rs
use std::collections::{HashMap, HashSet};
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub name: String,
    pub deps: Vec<String>,
    pub command: String,
}

/// 依存関係を解析して、安全な実行順（パス）に並べる
pub fn resolve_execution_order(tasks: &[Task]) -> Result<Vec<Task>, String> {
    let task_map: HashMap<String, Task> = tasks.iter().map(|t| (t.name.clone(), t.clone())).collect();
    let mut visited = HashSet::new();
    let mut temp_visited = HashSet::new();
    let mut order = Vec::new();

    fn visit(
        name: &str,
        task_map: &HashMap<String, Task>,
        visited: &mut HashSet<String>,
        temp_visited: &mut HashSet<String>,
        order: &mut Vec<Task>,
    ) -> Result<(), String> {
        if temp_visited.contains(name) {
            return Err(format!("循環参照（デッドロック）を検出しました: {}", name));
        }
        if !visited.contains(name) {
            temp_visited.insert(name.to_string());
            
            if let Some(task) = task_map.get(name) {
                for dep in &task.deps {
                    visit(dep, task_map, visited, temp_visited, order)?;
                }
            } else {
                return Err(format!("未定義の依存タスクです: {}", name));
            }

            temp_visited.remove(name);
            visited.insert(name.to_string());
            order.push(task_map.get(name).unwrap().clone());
        }
        Ok(())
    }

    for task in tasks {
        if !visited.contains(&task.name) {
            visit(&task.name, &task_map, &mut visited, &mut temp_visited, &mut order)?;
        }
    }

    Ok(order)
}

// =================================================================
// 【プロ仕様化 ④】Executor（実行環境）の分離・抽象化
// =================================================================

pub trait Executor {
    /// タスクを受け取り、その実行成否を返す
    fn execute(&self, task: &Task) -> bool;
}

/// ローカル環境で実行を担当する構造体（現在はログ出力のみ）
pub struct LocalExecutor;

impl Executor for LocalExecutor {
    fn execute(&self, task: &Task) -> bool {
        println!("  ⚡ [LocalExecute] ➔ [{}] Running: {}", task.name, task.command);
        // 現状は常に成功扱い。将来的に std::process::Command の実行結果などに差し替える
        true 
    }
}