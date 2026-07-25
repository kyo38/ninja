# Scion → Ninja

Distributed DAG Execution Engine (Experimental)

![OS: Windows 11](https://img.shields.io/badge/OS-Windows%2011-blue?style=flat-square&logo=windows11)
![Language: Rust](https://img.shields.io/badge/Language-Rust-orange?style=flat-square&logo=rust)
![IDE: VS Code](https://img.shields.io/badge/IDE-VS%20Code-007ACC?style=flat-square&logo=visualstudiocode)

---

## ■ Overview

This project is an experimental distributed execution engine based on **DAG (Directed Acyclic Graph)** task dependencies, developed specifically for Windows 11 environments using Rust and Tokio.

It focuses on solving the "execution order guarantee problem" and ensuring network resilience in asynchronous environments through structured state synchronization, automatic reconnections, and heartbeat death detection.

---

## ■ Key Technical Highlights

This system ensures strict execution order and high availability in an asynchronous distributed environment using:
- **`thiserror` Integration**: Centralized, type-safe error handling across network and system layers.
- **Exponential Backoff Reconnections**: Automatic retry with exponential backoff on connection failure.
- **Bi-directional Heartbeat (PING/PONG)**: Proactive 5-second PINGs with 15-second timeout monitoring for silent disconnect detection.
- **Task Execution Timed-outs**: Isolated task cancellation using Tokio timeouts.

---

## ■ Current Status

* **Phase 1 to 3: Completed**
  * Core DAG execution ordering ✔
  * Modular architecture refactoring ✔
  * Graceful shutdown with `q` key monitoring ✔
* **Phase 4: Completed (Resilience & Error Handling)**
  * Custom type-safe error base (`thiserror`) ✔
  * Worker auto-reconnect with exponential backoff ✔
  * Bi-directional PING/PONG heartbeat & 15s timeout monitoring ✔
* **Phase 5: Planned (Type-Safe Protocol & DAG Scheduler)**
  * Binary protocol serialization (`serde` / `bincode`)
  * Topological sorting & in-degree DAG scheduler
* **Phase 6: Planned (Distributed State & Multi-Worker Control)**
  * Worker pool management & failover re-assignment
* **Phase 7: Planned (Client CLI & E2E Testing)**
  * YAML/JSON DAG parser & end-to-end integration tests

---

## ■ Prerequisites

* **OS:** Windows 11 (Pro / Home)
* **Toolchain:** Rust (stable-x86_64-pc-windows-msvc)
* **IDE:** VS Code (Recommended extension: `rust-analyzer`)

---

## ■ Architecture

    +---------+
    | Client  |
    +----+----+
         | (Port 9090)
         v
    +----+----+
    | Master  | (Orchestrator)
    +----+----+
         | (Port 9001)
  +------+------+
  |      |      |
  v      v      v
+---+  +---+  +---+
| W |  | W |  | W |  (Workers)
+---+  +---+  +---+

### Communication Protocol
- **Client → Master (Port 9090)**: Submit DAG tasks and definitions.
- **Master → Worker (Port 9001)**: Assign executable tasks and exchange heartbeats (`PING`/`PONG`).
- **Worker → Master**: Notify task execution results and status.

---

## ■ Task Definition Example

    {
      "tasks": [
        { "id": "A", "deps": [] },
        { "id": "B", "deps": [] },
        { "id": "C", "deps": ["A"] },
        { "id": "D", "deps": ["B", "C"] }
      ]
    }

---

## ■ How to Run (Windows 11 / VS Code)

Run the following commands in VS Code integrated terminal (PowerShell):

    # 1. Clone repository
    git clone https://github.com/kyo38/ninja.git
    cd ninja

    # 2. Start Master node (Orchestrator)
    cargo run --bin ninja

    # 3. Start Workers (Open multiple terminals to enable parallelism)
    cargo run --bin worker

> **Note:** To shut down the Master and Worker nodes safely, type `q` and press `Enter` in their respective terminals. Debug logs can be enabled using `$env:RUST_LOG="debug"`.

---

## ■ Roadmap

* **Phase 5 (Type-Safe Protocol & Distributed Scheduler):**
  * Binary frame protocol (`bincode` / `serde`)
  * Topological sorting & in-degree DAG scheduler
* **Phase 6 (Distributed State & Multi-Worker Optimization):**
  * Worker pool manager (`WorkerManager`)
  * Parallel load balancing & failover re-assignment
* **Phase 7 (Client CLI & E2E Testing):**
  * YAML/JSON DAG definition parser
  * Complete E2E pipeline validation

---

## ■ Tech Stack

* **Language:** Rust
* **Async Runtime:** Tokio
* **Architecture:** Distributed Systems / DAG Scheduling
* **Target OS:** Windows 11

---

## ■ Project Purpose

- Deepen understanding of asynchronous distributed system architecture.
- Build a robust DAG task execution model in Rust.
- Enhance systems engineering and concurrency design skills.