# Scion → Ninja

分散DAG実行基盤 実験プロジェクト

![OS: Windows 11](https://img.shields.io/badge/OS-Windows%2011-blue?style=flat-square&logo=windows11)
![Language: Rust](https://img.shields.io/badge/Language-Rust-orange?style=flat-square&logo=rust)
![IDE: VS Code](https://img.shields.io/badge/IDE-VS%20Code-007ACC?style=flat-square&logo=visualstudiocode)

---

## ■ 概要

本プロジェクトは、DAG（有向非巡回グラフ）に基づくタスク依存関係を分散環境で安全に実行するための制御基盤です。Windows 11 動作環境に最適化し、Rust および Tokio を用いて開発されています。

非同期ネットワーク環境における「依存関係の順序保証」と「サイレント切断に対する耐障害性（リトライ・ハートビート・自動再割り当て）」に加え、構造化ログによる可観測性と組み込み Web ダッシュボードによるリアルタイムモニタリングを備えた堅牢なシステムです。

---

## ■ 技術的特徴

非同期分散環境でも確実な実行と高可用性を可能にするメカニズムを実装しています。
- **`thiserror` による統合エラー基盤**: ネットワーク・OS・シリアライズの各種エラーを型安全に統一管理
- **指数バックオフ自動再接続**: Master 切断時、Worker が指数バックオフで自動リトライ
- **双方向ハートビート & 自動フェイルオーバー**: PING/PONG 通信と 12秒タイムアウト監視で死体検知し、未完了タスクを自動再キューイング
- **`tracing::span` 構造化ログ**: タスクIDや Worker 情報を紐付けた高度な可観測性
- **組み込み Web ダッシュボード (Axum)**: リアルタイム HTML/JS UI と JSON REST API による進捗・状態監視

---

## ■ 現在のステータス

* **Phase 1 〜 3: 完了**
  * DAG順序保証 & 非同期バグ修正 ✔
  * モジュール構造のリファクタリング ✔
  * `q` キー監視による安全なシャットダウン ✔
* **Phase 4: 完了（信頼性・耐障害性の強化）**
  * `thiserror` による型安全エラーハンドリング ✔
  * 指数バックオフ付き Worker 自動再接続 ✔
* **Phase 5: 完了（型安全プロトコル & 分散DAGスケジューラ）**
  * 長さプレフィックス付き JSON プロトコル (`serde_json`) ✔
  * 入次数計算による動的 DAG スケジューラ ✔
* **Phase 6: 完了（分散状態管理 & フェイルオーバー）**
  * `tracing::span` による構造化コンテキストログ ✔
  * 12秒ハートビート死体検知 & Worker 自動除去 ✔
  * 離脱 Worker タスクの回収・稼働中 Worker への再割り当て ✔
* **Phase 7: 完了（Axum Web Dashboard & HTTP API）**
  * HTTP REST API (`/api/status`, `/api/workers`, `/api/tasks`) ✔
  * リアルタイム Web ダッシュボード UI (`http://127.0.0.1:8080`) ✔

---

## ■ 動作要件・前提環境

* **OS:** Windows 11 (Pro / Home)
* **Toolchain:** Rust (stable-x86_64-pc-windows-msvc)
* **IDE:** VS Code (推奨拡張機能: `rust-analyzer`)

---

## ■ アーキテクチャ

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

### 通信プロトコル
- **Client → Master (Port 9090)**: DAGタスク定義の投入
- **Master → Worker (Port 9001)**: 実行可能タスク割り当て・結果受信・ハートビート通信
- **Master → Browser (Port 8080)**: Axum によるリアルタイム Web UI & JSON REST API

---

## ■ タスク定義（例）

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

## ■ 実行手順 (Windows 11 / VS Code)

VS Code 上の統合ターミナル（PowerShell）で以下の手順を実行します。

```powershell
# 1. リポジトリを取得
git clone [https://github.com/kyo38/ninja.git](https://github.com/kyo38/ninja.git)
cd ninja

# 2. Master (Orchestrator) を起動
cargo run --bin ninja

# 3. Worker を起動 (複数のターミナルを開いて起動可能)
cargo run --bin worker

# 4. Client から DAG タスクを投入
cargo run --bin client

# 5. ブラウザでダッシュボードを開く
# [http://127.0.0.1:8080](http://127.0.0.1:8080)
```

> **注記:** Master ノードおよび Worker ノードを安全にシャットダウンするには、それぞれのターミナル画面で `q` を入力して `Enter` を押してください。デバッグログは `$env:RUST_LOG="info"` で有効化できます。

---

## ■ 今後のロードマップ

* **Phase 8 (外部 DAG 定義ファイル & CLI 機能の拡張):**
  * YAML/TOML 定義ファイルのパーサー
  * Command-line 引数解析 (`clap`)
  * 自動 E2E テストパイプラインの統合

---

## ■ 技術スタック

* **言語:** Rust
* **非同期ランタイム:** Tokio
* **Web フレームワーク:** Axum / Tower-HTTP
* **可観測性:** Tracing / Tracing-Subscriber
* **アーキテクチャ:** 分散システム / DAG スケジューリング
* **対象OS:** Windows 11

---

## ■ プロジェクトの目的

- 非同期分散処理アーキテクチャの正確な理解
- Rust における堅牢な DAG 実行モデルの実装
- 構造化ログ、非同期ネットワークプロトコル設計、リアルタイム Web モニタリング技術の習得

## ■ 今後の展望 (Future Work)

- **Master ノードの冗長化 (HA)**: Raft 合意アルゴリズム等を用いた Leader Election を導入し、Master の SPoF（単一障害点）を排除
- **動的 DAG グラフ変形**: 実行時のタスク状態に応じたグラフ構造の動的変更・条件分岐サポート
- **永続化ストレージのプラグイン化**: オプションで SQLite/RocksDB 等を統合し、実行履歴や監査ログの永続化に対応