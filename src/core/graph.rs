// src/core/graph.rs
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct Task {
    pub name: String,
    pub deps: Vec<String>,
    pub command: String,
}

/// 依存関係を解析して、安全な実行順（パス）に並べる
pub fn resolve_execution_order(tasks: &[Task]) -> Result<Vec<Task>, String> {
    // 警告対応: mut を削除
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