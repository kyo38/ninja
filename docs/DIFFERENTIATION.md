# Differentiation & Technical Positioning

## Problem

Existing distributed task systems such as Airflow, Ray, and Kubernetes Jobs are powerful, but often introduce significant operational complexity, heavy dependencies, and high runtime overhead—making them less suitable for lightweight, experimental, or secure environments.

## Approach

**Ninja** is designed as a:

- **Lightweight**
- **Rust-based**
- **Distributed DAG execution engine**

with a sharp focus on simplicity, strict execution control, and extensibility.

---

## Architecture Overview

```text
                 +-------------------+
                 | Web Browser / UI  | (Port 8080)
                 +---------+---------+
                           | (HTTP / JSON API)
     +---------+           v           +---------+
     | Client  | ----> +-------+ <---- | Workers |
     +---------+       |Master |       +---------+
    (Port 9090)        +-------+       (Port 9001)
 (Submit DAG Tasks)  (Orchestrator)  (Execute Tasks & Heartbeat)
```

---

## Key Differences

### 1. Lightweight by Design
No heavy external dependencies (no databases like PostgreSQL required) and minimal runtime overhead. Built in Rust with a memory footprint of just a few megabytes.

### 2. DAG-first Execution Model
Explicit dependency resolution with dynamic topological sorting and in-degree scheduling for exact execution order guarantees.

### 3. True Distributed Orchestration
Centralized master orchestrator with dynamic worker pool management, bi-directional 12-second PING/PONG heartbeat detection, and automatic task re-queuing on worker failure.

### 4. Observability Built-in
Structured tracing context (`tracing::span`) across asynchronous task dispatches, paired with an embedded real-time Axum Web Dashboard.

### 5. Extensibility
Plugin-ready architecture supporting external task definitions (JSON / YAML / TOML) and native OS process execution without runtime binding constraints.

---

## Comparison Matrix

| Aspect | Apache Airflow | Ray | Kubernetes Jobs | **Scion → Ninja** |
| :--- | :--- | :--- | :--- | :--- |
| **Primary Use Case** | Heavy ETL / Batch Processing | Python ML/AI Distributed Computing | Containerized Isolated Tasks | **Lightweight, Sub-second DAG Execution** |
| **Language & Runtime** | Python | Python / C++ | Container Runtime (Docker, etc.) | **Rust (Tokio)** |
| **Infrastructure Deps** | External DB (PostgreSQL), Web Server | Ray Cluster / Head Node | Kubernetes Cluster | **None (Single Binary)** |
| **Scheduling Granularity**| Minutes to Seconds | Milliseconds | Seconds to Minutes (Pod Overhead)| **Milliseconds** |
| **System Overhead** | High (DB Sync & Python Processes) | Medium (Ray Agent Management) | Extremely High (Pod Startup/Teardown) | **Minimal (TCP / JSON Frame Protocol)** |
| **Target Environment** | Linux-Centric | Linux-Centric | K8s Cluster Required | **Windows 11** |

---

## Positioning

Ninja is not intended to replace large-scale orchestration systems like Airflow or Kubernetes, but to complement them in scenarios where:

- **Simplicity is required**: Zero complex setup, no external database dependencies, and minimal operational overhead.
- **Full control is needed**: Direct process execution, explicit DAG ordering, custom timeout control, and low-level protocol handling.
- **Infrastructure is constrained**: Ideal for edge nodes, air-gapped networks, local development environments, and resource-restricted Windows 11 / Linux setups.

It is particularly optimized for:
- **Research & Experimental Environments**: Rapid prototyping of asynchronous distributed algorithms.
- **Secure & Air-gapped Systems**: Standalone execution without external cloud or container runtime dependencies.
- **Embedded Execution Engines**: Integrating sub-millisecond DAG scheduling directly into engineering toolchains.
