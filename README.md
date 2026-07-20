SCIONからNinjaへ
Rustによる次世代セキュア経路制御基盤
開発ロードマップ（改訂版）

本書は、Path-Aware Networkingの思想を基盤として、
Rustによる次世代セキュア経路制御基盤を構築するための
開発指針をまとめたものである。

本プロジェクトのソースコード：

 Master（Control Plane）
 Worker（Execution Node / Data Plane）
 Client（Trigger）

 DAG（依存関係グラフ）
 Event-driven実行

[basicstyle=]
        +---------+
        | Client  |
        +----+----+
             |
             v
        +---------+      TCP (9001)
        | Master  |<------------------+
        +----+----+                   |
             |                        |
             | TCP (9000)             |
             v                        |
        +---------+                   |
        | Worker  |-------------------+
        +---------+

[制御フロー]
Client -> Master -> Worker

[状態同期]
Worker -> Master (Notify)

本分散システムのテストを行うため、VSCodeのターミナル（PowerShell等）を「3つ」開き、以下の順序でコンポーネントを起動する。

[language=bash]
git clone https://github.com/kyo38/ninja
cd ninja
cargo check

[language=bash]
cargo run --bin ninja

ポート9001（対Worker）および9000（対Client）で待機状態となる。

[language=bash]
cargo run --bin worker

起動後、自動的にMasterへソケット接続し、クラスタへのチェックインを完了して指示を待機する。

[language=bash]
cargo run --bin client

4つのタスクを含むJSONパケットをMasterへ流し込み、即座に離脱（正常終了）する。

タスク投入時、MasterとWorkerの間でフライングのない厳密な順序保証が機能していることを確認する。

  の受信・実行・Masterへの完了報告
  の受信・実行・Masterへの完了報告
 （Aに依存）の受信・実行・Masterへの完了報告
 （最終リーフ）の受信・実行・Masterへの完了報告

実証中にさらに「4つ目のターミナル」を立ち上げ、2基目のWorkerを起動する（）。この状態でClientからタスクを再投入することで、Masterが並列可能なタスク（AとB）を異なるWorkerノードへ同時に動的分配する「並列負荷分散」の挙動を直接観察可能である。

 Success：正常終了
 Failure：論理的失敗（再試行対象）
 InfraError：インフラ障害（再試行対象）

失敗とインフラ障害を分離することで、
リトライ戦略の精密制御が可能となる。

初期実装では以下の問題が発生した：

 Worker実行中に次タスクが送信される
 DAG依存を無視したフライング実行

非同期処理においてタスク完了を待っていなかった

 state\_mapによる状態管理
 Notifyによる完了同期

これにより順序保証が実現された。

 GUI
 永続化
 完全分散合意

 DAGベース実行
 Master/Worker/Client分離
 TCP通信
 レースコンディション修正

 タスク送信と完了は非同期
 状態同期が必須

 DAG順序保証
 非同期バグ非再現

 リトライ
 タイムアウト
 並列化

 TaskResultに基づく再試行
 backoff実装
 並列Worker検証

 ノード発見
 状態監視
 再スケジューリング

 WireGuard
 PQC
 鍵管理

 組込み
 Fail-safe
 自動復旧

 Phase2はPhase1に依存
 Phase3はPhase2に依存
 Phase4はControl Plane後

 DAG最適化
 分散化
 観測（ログ・メトリクス）
