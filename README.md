# Scion → Ninja

Distributed DAG Execution Engine (Experimental)

![OS: Windows 11](https://img.shields.io/badge/OS-Windows%2011-blue?style=flat-square&logo=windows11)
![Language: Rust](https://img.shields.io/badge/Language-Rust-orange?style=flat-square&logo=rust)
![IDE: VS Code](https://img.shields.io/badge/IDE-VS%20Code-007ACC?style=flat-square&logo=visualstudiocode)

---

## ■ Overview

This project is an experimental distributed execution engine based on **DAG (Directed Acyclic Graph)** task dependencies, developed specifically for Windows 11 environments using Rust and Tokio.

It focuses on solving the "execution order guarantee problem" in asynchronous environments by combining:
- Dependency resolution
- State synchronization
- Notification-based completion

---

## ■ Key Technical Highlights

This system ensures strict execution order in an asynchronous distributed environment using:
- **`state_map`**: For real-time task state tracking.
- **`notify` mechanism**: For explicit completion signaling across tasks.

This eliminates race conditions and premature execution ("flying execution").

---

## ■ Current Status

* **Phase 1: Completed**
  * DAG execution ordering ✔
  * Async bug fix ✔
  * Core execution flow ✔
* **Phase 2: In Progress**
  * Retry mechanism (TODO)
  * Timeout control (TODO)
  * Parallelism control (Testing)
* **Phase 3: Planned**
  * Distributed scheduling
  * Auto-scaling workers
* **Phase 4: Planned**
  * Security
  * Authentication / Authorization

---

## ■ Prerequisites

* **OS:** Windows 11 (Tested & Optimized)
* **Toolchain:** Rust (stable-x86_64-pc-windows-msvc)
* **Environment:** VS Code (Recommended extension: `rust-analyzer`)

---

## ■ Architecture

```text
          +---------+
          | Client  |
          +----+----+
               |
               v
          +----+----+
          | Master  |
          +----+----+
               |
   +-----------------------+
   |           |           |
   v           v           v
 +--------+  +--------+  +--------+
 | Worker |  | Worker |  | Worker |
 +--------+  +--------+  +--------+
```

### Communication Protocol
- **Client → Master**: Submit DAG tasks
- **Master → Worker**: Assign executable tasks
- **Worker → Master**: Notify execution completion

---

## ■ Task Definition Example

```json
{
  "tasks": [
    { "id": "A", "deps": [] },
    { "id": "B", "deps": [] },
    { "id": "C", "deps": ["A"] },
    { "id": "D", "deps": ["B", "C"] }
  ]
}
```

---

## ■ Execution Model & Lifecycle

1. **Master** manages all task definitions and global DAG state.
2. Dependencies are dynamically resolved.
3. Only executable tasks (with all dependencies met) are assigned to **Workers**.
4. **Workers** execute tasks asynchronously.
5. Worker state transitions: `Idle` → `Assigned` → `Running` → `Completed` → `Notify`.
6. Completion is notified back to Master.
7. `state_map` updates unlock the next dependent tasks.

---

## ■ Async Bug & Technical Resolution

* **Problem:** Tasks were executed before their dependencies were completed (*premature execution*).
* **Root Cause:** Lack of proper synchronization in async runtime processing.
* **Fix Applied:**
  * Introduced centralized `state_map` for state tracking.
  * Added explicit `notify` mechanisms.
  * Implemented rigorous dependency resolution logic.
* **Result:** Achieved strict DAG execution order guarantee.

---

## ■ How to Run (Windows 11 / VS Code)

Run the following commands in VS Code integrated terminal (PowerShell):

```powershell
# 1. Clone repository
git clone [https://github.com/kyo38/ninja.git](https://github.com/kyo38/ninja.git)
cd ninja

# 2. Start Master node
cargo run --bin master

# 3. Start Workers (Open multiple terminals to enable parallelism)
cargo run --bin worker

# 4. Submit tasks via Client
cargo run --bin client
```

---

## ■ Parallel Execution

- Running multiple Worker processes enables parallel processing.
- Independent tasks in the DAG execute concurrently across available Workers.

---

## ■ Roadmap

* **Phase 2 (Reliability):**
  * Retry mechanism
  * Timeout control
  * Concurrency limits
* **Phase 3 (Scalability):**
  * Distributed scheduling
  * Sharding
  * Queue optimization
* **Phase 4 (Security):**
  * Authentication (Auth)
  * Authorization (RBAC)
  * Secure communication

---

## ■ Tech Stack

* **Language:** Rust
* **Async Runtime:** Tokio
* **Architecture:** Distributed Systems / DAG Scheduling
* **Target OS:** Windows 11

---

## ■ Project Purpose

- Deepen understanding of asynchronous distributed system architecture.
- Build a robust, production-grade DAG task execution model in Rust.
- Enhance systems engineering and concurrency design skills.
