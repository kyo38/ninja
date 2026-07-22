Scion → Ninja Roadmap
Distributed DAG Execution Engine (Experimental)
==============================================

■ Overview
--------------------------------------------------
This project is an experimental distributed execution engine
based on DAG (Directed Acyclic Graph) task dependencies.

It focuses on solving the "execution order guarantee problem"
in asynchronous environments by combining:

- Dependency resolution
- State synchronization
- Notification-based completion

--------------------------------------------------
■ What Makes This Interesting
--------------------------------------------------
This system ensures strict execution order in an asynchronous
distributed environment using:

- state_map for task state tracking
- notify mechanism for completion signaling

This eliminates race conditions and premature execution.

--------------------------------------------------
■ Current Status
--------------------------------------------------

Phase 1: Completed
- DAG execution ordering ✔
- Async bug fix ✔
- Core execution flow ✔

Phase 2: In Progress
- Retry mechanism (TODO)
- Timeout control (TODO)
- Parallelism control (Testing)

Phase 3: Planned
- Distributed scheduling
- Auto-scaling workers

Phase 4: Planned
- Security
- Authentication / Authorization

--------------------------------------------------
■ Architecture
--------------------------------------------------

          +---------+
          | Client  |
          +----+----+
               |
               v
          +----+----+
          | Master  |
          +----+----+
               |
     -------------------------
     |           |           |
     v           v           v
 +--------+  +--------+  +--------+
 | Worker |  | Worker |  | Worker |
 +--------+  +--------+  +--------+

Communication:
- Client → Master : Submit tasks
- Master → Worker : Assign tasks
- Worker → Master : Notify completion

--------------------------------------------------
■ Task Definition (Example)
--------------------------------------------------

{
  "tasks": [
    { "id": "A", "deps": [] },
    { "id": "B", "deps": [] },
    { "id": "C", "deps": ["A"] },
    { "id": "D", "deps": ["B", "C"] }
  ]
}

--------------------------------------------------
■ Execution Model
--------------------------------------------------

1. Master manages all tasks
2. Dependencies are resolved
3. Only executable tasks are assigned
4. Workers execute tasks
5. Completion is notified back to Master
6. state_map updates unlock next tasks

--------------------------------------------------
■ Worker State Transition
--------------------------------------------------

Idle → Assigned → Running → Completed → Notify

--------------------------------------------------
■ Async Bug & Fix
--------------------------------------------------

Problem:
Tasks were executed before dependencies were completed
("premature execution")

Cause:
Lack of proper synchronization in async processing

Fix:
- Introduced state_map for tracking
- Added notify mechanism
- Implemented dependency resolution

Result:
Strict DAG execution order achieved

--------------------------------------------------
■ How to Run
--------------------------------------------------

1. Clone repository

   git clone <repo>

2. Start Master

   cargo run --bin master

3. Start Workers (multiple)

   cargo run --bin worker

4. Submit tasks

   cargo run --bin client

--------------------------------------------------
■ Parallel Execution
--------------------------------------------------

- Run multiple workers to enable parallelism
- Independent tasks execute concurrently

--------------------------------------------------
■ Roadmap
--------------------------------------------------

Phase 2:
- Retry
- Timeout control
- Concurrency limits

Phase 3:
- Distributed scheduling
- Sharding
- Queue optimization

Phase 4:
- Authentication
- Authorization (RBAC)
- Secure communication

--------------------------------------------------
■ Tech Stack
--------------------------------------------------

- Rust
- Async runtime (Tokio)
- Distributed system design
- DAG scheduling

--------------------------------------------------
■ Purpose
--------------------------------------------------

- Understand async distributed systems
- Implement DAG execution model
- Build production-level design skills



