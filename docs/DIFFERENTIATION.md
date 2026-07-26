# Scion → Ninja: Positioning & Technical Differentiation

This document outlines the architectural positioning and key differentiators of `ninja` compared to major existing distributed execution engines and workflow orchestrators (Apache Airflow, Ray, and Kubernetes Jobs).

---

## 1. Design Philosophy

`ninja` is engineered specifically as a **lightweight, sub-millisecond, single-binary distributed DAG execution engine**. It eliminates the need for heavy infrastructure overhead, external databases, or complex setup procedures, making it ideal for Windows 11, local development, edge node, and air-gapped environments.

---

## 2. Comparison Matrix

| Aspect | Apache Airflow | Ray | Kubernetes Jobs | **Scion → Ninja** |
| :--- | :--- | :--- | :--- | :--- |
| **Primary Use Case** | Heavy ETL / Batch Processing | Python ML/AI Distributed Computing | Containerized Isolated Tasks | **Lightweight, Sub-second DAG Execution** |
| **Language & Runtime** | Python | Python / C++ | Container Runtime (Docker, etc.) | **Rust (Tokio)** |
| **Infrastructure Deps** | External DB (PostgreSQL), Web Server | Ray Cluster / Head Node | Kubernetes Cluster | **None (Single Binary)** |
| **Scheduling Granularity**| Minutes to Seconds | Milliseconds | Seconds to Minutes (Pod Overhead)| **Milliseconds** |
| **System Overhead** | High (DB Sync & Python Processes) | Medium (Ray Agent Management) | Extremely High (Pod Startup/Teardown) | **Minimal (TCP / JSON Frame Protocol)** |
| **Target Environment** | Linux-Centric | Linux-Centric | K8s Cluster Required | **Windows 11 / Linux (Edge Compatible)** |

---

## 3. Core Value Propositions

### ① Zero External Dependencies & Low Footprint
No external database, container runtime, or Python interpreter is required. Built entirely in Rust, `ninja` operates as a single executable with a memory footprint of just a few megabytes.

### ② Sub-millisecond Dispatching for Local & Air-Gapped Environments
In environments where deploying Kubernetes or cloud infrastructure is impractical—such as factory networks, edge computing nodes, or isolated Windows 11 setups—`ninja` guarantees strict execution ordering with sub-millisecond task dispatching.

### ③ Language-Agnostic Process Orchestration
Unlike Python-bound frameworks, `ninja` seamlessly orchestrates native OS binaries, scripts, and commands as nodes in a DAG without runtime binding constraints.

---

## 4. Elevator Pitch

> "A single-binary, Rust-based distributed DAG scheduler designed for local, edge, and air-gapped environments that demand strict execution ordering, sub-millisecond dispatching, bi-directional heartbeat detection, and dynamic failover—without the infrastructure overhead of Airflow or Kubernetes."