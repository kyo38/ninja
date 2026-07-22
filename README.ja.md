# Scion → Ninja

分散DAG実行基盤 実験プロジェクト

![OS: Windows 11](https://img.shields.io/badge/OS-Windows%2011-blue?style=flat-square&logo=windows11)
![Language: Rust](https://img.shields.io/badge/Language-Rust-orange?style=flat-square&logo=rust)
![IDE: VS Code](https://img.shields.io/badge/IDE-VS%20Code-007ACC?style=flat-square&logo=visualstudiocode)

---

## ■ 概要

本プロジェクトは、DAG（有向非巡回グラフ）に基づくタスク依存関係を分散環境で安全に実行するための制御基盤です。Windows 11 動作環境に最適化し、Rust および Tokio を用いて開発されています。

非同期環境における「順序保証問題」を解決するため、状態同期と依存解決を組み合わせた高精度な実行モデルを実装しています。

---

## ■ 技術的特徴

DAGベース実行と状態同期メカニズムにより、非同期環境でも確実な実行順序保証を実現しています。

- **`state_map`**: タスクのリアルタイムな状態追跡と管理
- **`notify` 機構**: タスク完了イベントの確実な通知と制御

これにより、非同期処理特有の競合状態（レースコンディション）やフライング実行を完全に排除します。

---

## ■ 現在のステータス

* **Phase 1: 完了**
  * DAG順序保証 ✔
  * 非同期バグ修正 ✔
  * 基本実行フロー ✔
* **Phase 2: 開発中**
  * リトライ機構（未）
  * タイムアウト制御（未）
  * 並列制御（検証中）
* **Phase 3: 未着手**
  * 分散スケジューリング
  * Worker自動スケール
* **Phase 4: 未着手**
  * セキュリティ
  * 認証 / 認可

---

## ■ 動作要件・前提環境

* **OS:** Windows 11 (Pro / Home)
* **Toolchain:** Rust (stable-x86_64-pc-windows-msvc)
* **IDE:** VS Code (推奨拡張機能: `rust-analyzer`)

---

## ■ アーキテクチャ

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

### 通信プロトコル
- **Client → Master**: タスク定義・DAG投入
- **Master → Worker**: 実行可能タスクの割当
- **Worker → Master**: タスク完了通知

---

## ■ タスク定義（例）

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

## ■ 実行モデルとライフサイクル

1. **Master** が全タスク定義とDAG状態を一括管理。
2. 依存関係を解決。
3. すべての依存が満たされた実行可能タスクのみを **Worker** へ割り当て。
4. **Worker** がタスクを非同期実行。
5. Worker状態遷移: `Idle` → `Assigned` → `Running` → `Completed` → `Notify`
6. 完了通知を Master へ返答。
7. `state_map` を更新し、後続タスクのロックを解除。

---

## ■ 非同期バグと修正（トラブルシューティング履歴）

* **問題:** 依存タスクが完了する前に後続タスクが実行される（フライング実行）。
* **原因:** 非同期処理（Async Runtime）において厳密な完了待ちが保証されていなかった。
* **修正策:**
  * `state_map` による状態追跡の導入
  * `notify` による完了制御の実装
  * 依存解決ロジックの再設計
* **結果:** 完全なDAG実行順序保証を達成。

---

## ■ 実行手順 (Windows 11 / VS Code)

VS Code上の統合ターミナル（PowerShell）で以下の手順を実行します。

```powershell
# 1. リポジトリを取得
git clone [https://github.com/kyo38/ninja.git](https://github.com/kyo38/ninja.git)
cd ninja

# 2. Masterを起動
cargo run --bin master

# 3. Workerを起動 (複数のターミナルを開いて起動可能)
cargo run --bin worker

# 4. Clientからタスク投入
cargo run --bin client
```

---

## ■ 並列実行確認

- Workerプロセスを複数起動することで、自動的に分散並列処理が行われます。
- DAGにおいて依存関係のない独立したタスクは同時に並列実行されます。

---

## ■ 今後のロードマップ

* **Phase 2 (信頼性向上):**
  * Retry（再試行）
  * Timeout制御
  * 並列数制限
* **Phase 3 (拡張性向上):**
  * 分散スケジューリング
  * シャーディング
  * キュー最適化
* **Phase 4 (セキュリティ):**
  * 認証（Auth）
  * 認可（RBAC）
  * セキュア通信

---

## ■ 技術スタック

* **言語:** Rust
* **非同期ランタイム:** Tokio
* **設計コンセプト:** 分散システム / DAGスケジューリング
* **対象OS:** Windows 11

---

## ■ プロジェクトの目的

- 非同期分散処理アーキテクチャの正確な理解
- Rustにおける堅牢なDAG実行モデルの実装
- 実務レベルのシステム設計力・並行処理スキルの向上
