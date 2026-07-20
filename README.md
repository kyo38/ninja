SCIONからNinjaへ：
Rustによる次世代セキュア経路制御基盤
開発ロードマップ（改訂版）
July 20, 2026
1 目的
本書は、Path-Aware Networking の思想を基盤として、Rust による次世代セキュア経路
制御基盤を構築するための開発指針をまとめたものである。
2 リポジトリ
本プロジェクトのソースコード：
• https://github.com/kyo38/ninja
3 アーキテクチャ概要
3.1 構成要素
• Master（Control Plane）
• Worker（Execution Node / Data Plane）
• Client（Trigger）
3.2 実行モデル
• DAG（依存関係グラフ）
• Event-driven 実行
4 システム構成図
+---------+
| Client |
+----+----+
|v
1
+---------+ TCP (9001)
| Master |<------------------+
+----+----+ |
| |
| TCP (9000) |
v |
+---------+ |
| Worker |-------------------+
+---------+
[制御フロー]
Client -> Master -> Worker
[状態同期]
Worker -> Master (Notify)
5 実験（ビルド・テスト）手順
本分散システムのテストを行うため、VSCode のターミナル（PowerShell 等）を「3 つ」開
き、以下の順序でコンポーネントを起動する。
1. リポジトリの準備と確認
g i t c l o n e h t t p s : / / g i t h u b . com/ kyo38 / n i n j a
cd n i n j a
c a r g o check
2. 【ターミナル1】Master サーバーの起動
c a r g o run −−bin n i n j a
ポート9001（対Worker）および9000（対Client）で待機状態となる。
3. 【ターミナル2】Worker ノードの起動
c a r g o run −−bin worker
起動後、自動的にMaster へソケット接続し、クラスタへのチェックインを完了して
指示を待機する。
4. 【ターミナル3】Client からのDAG タスク投入
c a r g o run −−bin c l i e n t
4 つのタスクを含むJSON パケットをMaster へ流し込み、即座に離脱（正常終了）
する。
5.1 期待される検証結果（正常なログストリーム）
タスク投入時、Master とWorker の間でフライングのない厳密な順序保証が機能している
ことを確認する。
• 期待されるWorker 側のログ遷移：
2
1. Task_B の受信・実行・Master への完了報告
2. Task_A の受信・実行・Master への完了報告
3. Task_C（A に依存）の受信・実行・Master への完了報告
4. Task_D（最終リーフ）の受信・実行・Master への完了報告
5.2 プロテスター向け追加検証（マルチワーカー負荷分散）
実証中にさらに「4 つ目のターミナル」を立ち上げ、2 基目のWorker を起動する（cargo
run --bin worker）。この状態でClient からタスクを再投入することで、Master が並列
可能なタスク（A とB）を異なるWorker ノードへ同時に動的分配する「並列負荷分散」の
挙動を直接観察可能である。
6 設計上の重要概念
6.1 TaskResult モデル
• Success：正常終了
• Failure：論理的失敗（再試行対象）
• InfraError：インフラ障害（再試行対象）
失敗とインフラ障害を分離することで、リトライ戦略の精密制御が可能となる。
7 非同期バグとその解決
初期実装では以下の問題が発生した：
• Worker 実行中に次タスクが送信される
• DAG 依存を無視したフライング実行
原因：
非同期処理においてタスク完了を待っていなかった
対策：
• state_map による状態管理
• Notify による完了同期
これにより順序保証が実現された。
8 非目標
• GUI
• 永続化
• 完全分散合意
3
9 開発フェーズ
9.1 Phase 1: 分散スケジューリング基盤（★完了）
達成内容：
• DAG ベース実行
• Master/Worker/Client 分離
• TCP 通信
• レースコンディション修正
技術的知見：
• タスク送信と完了は非同期
• 状態同期が必須
完了条件：
• DAG 順序保証
• 非同期バグ非再現
9.2 Phase 2: 実行制御強化（進行中）
• リトライ
• タイムアウト
• 並列化
完了条件：
• TaskResult に基づく再試行
• backoff 実装
• 並列Worker 検証
9.3 Phase 3: Control Plane
• ノード発見
• 状態監視
• 再スケジューリング
9.4 Phase 4: セキュリティ
• WireGuard
• PQC
• 鍵管理
4
9.5 Phase 5: 実運用
• 組込み
• Fail-safe
• 自動復旧
10 フェーズ依存
• Phase2 はPhase1 に依存
• Phase3 はPhase2 に依存
• Phase4 はControl Plane 後
11 将来拡張
• DAG 最適化
• 分散化
• 観測（ログ・メトリクス）
