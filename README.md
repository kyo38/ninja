# Ninja

Distributed DAG Execution Engine (Experimental)
Specification: v0.1

![OS: Windows 11](https://img.shields.io/badge/OS-Windows%2011-blue?style=flat-square&logo=windows11)
![Language: Rust](https://img.shields.io/badge/Language-Rust-orange?style=flat-square&logo=rust)
![IDE: VS Code](https://img.shields.io/badge/IDE-VS%20Code-007ACC?style=flat-square&logo=visualstudiocode)

---

## ■ Overview

This project is an experimental distributed execution engine based on **DAG (Directed Acyclic Graph)** task dependencies, developed specifically for Windows 11 environments using Rust and Tokio.

It focuses on solving the "execution order guarantee problem" and ensuring network resilience in asynchronous environments through structured state synchronization, automatic reconnections, heartbeat death detection, dynamic failover, and real-time monitoring via an embedded Web Dashboard.

---

## ■ Key Technical Highlights

This system ensures strict execution order and high availability in an asynchronous distributed environment using:
- **`thiserror` Integration**: Centralized, type-safe error handling across network and system layers.
- **Exponential Backoff Reconnections**: Automatic retry with exponential backoff on connection failure.
- **Bi-directional Heartbeat & Failover**: Proactive PINGs with 12-second timeout monitoring for silent disconnect detection and automatic task re-queuing.
- **Structured Observability (`tracing::span`)**: Context-bound tracing across asynchronous task dispatches.
- **Embedded Web Dashboard (Axum)**: Real-time HTML/JS UI and JSON REST API for live progress monitoring.

---

## ■ Current Status

* **Phase 1 to 3: Completed**
  * Core DAG execution ordering ✔
  * Modular architecture refactoring ✔
  * Graceful shutdown with `q` key monitoring ✔
* **Phase 4: Completed (Resilience & Error Handling)**
  * Custom type-safe error base (`thiserror`) ✔
  * Worker auto-reconnect with exponential backoff ✔
* **Phase 5: Completed (Type-Safe Protocol & DAG Scheduler)**
  * Length-prefixed JSON serialization (`serde_json`) ✔
  * Topological sorting & in-degree dynamic DAG scheduler ✔
* **Phase 6: Completed (Observability & Failover Engine)**
  * Structured tracing context (`tracing::span`) ✔
  * 12s Heartbeat timeout monitoring & automatic worker eviction ✔
  * Abandoned task recovery & re-queuing to active workers ✔
* **Phase 7: Completed (Axum Web Dashboard & HTTP API)**
  * HTTP REST API (`/api/status`, `/api/workers`, `/api/tasks`) ✔
  * Embedded HTML/JS real-time dashboard UI (`[http://127.0.0.1:8080](http://127.0.0.1:8080)`) ✔

---

## ■ Prerequisites

* **OS:** Windows 11 (Pro / Home)
* **Toolchain:** Rust (stable-x86_64-pc-windows-msvc)
* **IDE:** VS Code (Recommended extension: `rust-analyzer`)

---

## ■ Architecture

```text
                 +-------------------+
                 | Web Browser / UI  | (Port 8080)
                 +---------+---------+
                           |
     +---------+           v           +---------+
     | Client  | ----> +-------+ <---- | Workers |
     +---------+       |Master |       +---------+
     (Port 9090)       +-------+       (Port 9001)
```

### Communication Protocol
- **Client → Master (Port 9090)**: Submit DAG tasks and definitions.
- **Master → Worker (Port 9001)**: Assign executable tasks, track results, and exchange heartbeats.
- **Master → Browser (Port 8080)**: Axum real-time Web Dashboard & JSON REST API.

---

## ■ Task Definition Example

```json
[
  {
    "task_id": "task-1a",
    "command": "cmd",
    "args": ["/C", "echo Hello Task 1A"],
    "dependencies": []
  },
  {
    "task_id": "task-1b",
    "command": "cmd",
    "args": ["/C", "echo Hello Task 1B"],
    "dependencies": []
  },
  {
    "task_id": "task-2",
    "command": "cmd",
    "args": ["/C", "echo Task 2"],
    "dependencies": ["task-1a", "task-1b"]
  }
]
```

---

## ■ How to Run (Windows 11 / VS Code)

Run the following commands in VS Code integrated terminal (PowerShell):

```powershell
# 1. Clone repository
git clone https://github.com/kyo38/ninja.git
cd ninja

# 2. Start Master node (Orchestrator)
cargo run --bin ninja

# 3. Start Workers (Open multiple terminals to enable parallelism)
cargo run --bin worker

# 4. Submit DAG tasks
cargo run --bin client

# 5. Access Web Dashboard in Browser:
# http://127.0.0.1:8080
```

> **Note:** To shut down Master and Worker nodes safely, type `q` and press `Enter` in their respective terminals. Debug logs can be enabled using `$env:RUST_LOG="info"`.

---

## ■ Roadmap

* **Phase 8 (External DAG Definition & CLI Extension):**
  * YAML/TOML DAG file parser
  * Command-line argument parsing (`clap`)
  * Automated E2E test pipeline integration

---

## ■ Tech Stack

* **Language:** Rust
* **Async Runtime:** Tokio
* **Web Framework:** Axum / Tower-HTTP
* **Observability:** Tracing / Tracing-Subscriber
* **Architecture:** Distributed Systems / DAG Scheduling
* **Target OS:** Windows 11

---

## ■ Project Purpose

- Deepen understanding of asynchronous distributed system architecture.
- Build a robust DAG task execution model in Rust.
- Master structured observability, async network protocol design, and real-time Web monitoring.

## ■ Future Work & Architecture Roadmaps

- **Master High Availability (HA)**: Implement Leader Election (e.g., Raft consensus) to eliminate the Single Point of Failure (SPOF) on the Master node.
- **Dynamic Task Graph Updates**: Support dynamic DAG mutations and branching during runtime.
- **Pluggable Storage Backends**: Optional SQLite/RocksDB backend for persistent task execution history and audit logs.
