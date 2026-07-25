# Scion → Ninja

分散DAG実行基盤 実験プロジェクト

![OS: Windows 11](https://img.shields.io/badge/OS-Windows%2011-blue?style=flat-square&logo=windows11)
![Language: Rust](https://img.shields.io/badge/Language-Rust-orange?style=flat-square&logo=rust)
![IDE: VS Code](https://img.shields.io/badge/IDE-VS%20Code-007ACC?style=flat-square&logo=visualstudiocode)

---

## ■ 概要

本プロジェクトは、DAG（有向非巡回グラフ）に基づくタスク依存関係を分散環境で安全に実行するための制御基盤です。Windows 11 動作環境に最適化し、Rust および Tokio を用いて開発されています。

非同期ネットワーク環境における「依存関係の順序保証」と「サイレント切断に対する耐障害性（リトライ・ハートビート）」を兼ね備えた堅牢なシステムを構築しています。

---

## ■ 技術的特徴

非同期分散環境でも確実な実行と自立復旧を可能にする各種メカニズムを実装しています。

- **`thiserror` による統合エラー基盤**: ネットワーク・OS・シリアライズの各種エラーを型安全に統一管理
- **指数バックオフ自動再接続**: Master 切断時、Worker が `1s → 2s → 4s ... 30s` で自動リトライ
- **双方向ハートビート（PING/PONG）**: 5秒周期の PING と 15秒のタイムアウト監視でサイレント切断を即座に感知・クリーンアップ
- **タスク実行タイムアウト制御**: Tokio タイムアウトを用いた安全なプロセス監視

---

## ■ 現在のステータス

* **Phase 1 〜 3: 完了**
  * DAG順序保証 & 非同期バグ修正 ✔
  * モジュール構造のリファクタリング ✔
  * `q` キー監視による安全なシャットダウン ✔
* **Phase 4: 完了（信頼性・耐障害性の強化）**
  * `thiserror` による型安全エラーハンドリング ✔
  * 指数バックオフ付き Worker 自動再接続 ✔
  * 双方向 PING/PONG ハートビート & 15秒タイムアウト監視 ✔
* **Phase 5: 開発予定（型安全プロトコル & 分散DAGスケジューラ）**
  * バイナリプロトコル化 (`serde` / `bincode`)
  * トポロジカルソート & 入次数計算による DAG スケジューラ
* **Phase 6: 開発予定（分散状態管理 & マルチWorker制御）**
  * Worker プール管理 & 障害時のタスク再割り当て（フェイルオーバー）
* **Phase 7: 開発予定（クライアントCLI & E2E統合テスト）**
  * YAML/JSON 定義ファイルのパーサー & E2E テストの完成

---

## ■ 動作要件・前提環境

* **OS:** Windows 11 (Pro / Home)
* **Toolchain:** Rust (stable-x86_64-pc-windows-msvc)
* **IDE:** VS Code (推奨拡張機能: `rust-analyzer`)

---

## ■ アーキテクチャ

         +---------+
         | Client  |
         +----+----+
              | (Port 9090)
              v
         +----+----+
         | Master  | (Orchestrator)
         +----+----+
              | (Port 9001)
   +----------+----------+
   |          |          |
   v          v          v
+--------+ +--------+ +--------+
| Worker | | Worker | | Worker |
+--------+ +--------+ +--------+

### 通信プロトコル
- **Client → Master (Port 9090)**: DAGタスク定義の投入
- **Master → Worker (Port 9001)**: 実行可能タスクの割り当て & ハートビート通信 (`PING`/`PONG`)
- **Worker → Master**: タスク実行結果の報告

---

## ■ タスク定義（例）

{
  "tasks": [
    { "id": "A", "deps": [] },
    { "id": "B", "deps": [] },
    { "id": "C", "deps": ["A"] },
    { "id": "D", "deps": ["B", "C"] }
  ]
}

---

## ■ 実行手順 (Windows 11 / VS Code)

VS Code 上の統合ターミナル（PowerShell）で以下の手順を実行します。

git clone [https://github.com/kyo38/ninja.git](https://github.com/kyo38/ninja.git)
cd ninja

# Master (Orchestrator) を起動
cargo run --bin ninja

# Worker を起動 (複数のターミナルを開いて起動可能)
cargo run --bin worker

> **注記:** Master ノードおよび Worker ノードを安全にシャットダウンするには、それぞれのターミナル画面で `q` を入力して `Enter` を押してください。詳細なデバッグログを表示する場合は `$env:RUST_LOG="debug"` を設定して実行します。

---

## ■ 今後のロードマップ

* **Phase 5 (型安全プロトコル & 分散DAGスケジューラ):**
  * バイナリフレームプロトコル (`bincode` / `serde`)
  * トポロジカルソート & 入次数計算による DAG スケジューラ
* **Phase 6 (分散状態管理 & マルチWorker制御):**
  * Worker プール管理構造体 (`WorkerManager`)
  * 並列ロードバランシング & フェイルオーバー再割り当て
* **Phase 7 (クライアントCLI & E2E統合テスト):**
  * YAML/JSON 定義ファイルのパーサー
  * フルパイプラインの E2E 統合テスト

---

## ■ 技術スタック

* **言語:** Rust
* **非同期ランタイム:** Tokio
* **設計コンセプト:** 分散システム / DAGスケジューリング / 耐障害性設計
* **対象OS:** Windows 11

---

## ■ プロジェクトの目的

- 非同期分散処理アーキテクチャの正確な理解
- Rustにおける堅牢なDAG実行モデルの実装
- 実務レベルのシステム設計力・並行処理スキルの向上