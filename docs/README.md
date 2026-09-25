# 本地事件流处理引擎

纯 Go 标准库实现（`module github.com/h3yow/stream-engine-go`，无第三方依赖）的本地事件流处理引擎：
本地模拟数据源注入事件，按**事件时间**做滚动窗口聚合，以**水位线**驱动窗口定稿，
支持**检查点恢复**与**有界背压**，结果写入幂等的本地模拟下游。

```
pkg/event       事件类型与可区分的丢弃原因
pkg/watermark   有界乱序水位线生成器
pkg/window      滚动窗口分配与聚合状态（含 Seq 去重）
pkg/checkpoint  检查点结构与存储（内存实现）
pkg/sink        幂等收集型下游（可注入写延迟模拟慢下游）
pkg/source      确定性事件重放数据源
pkg/engine      编排：路由分片、水位线触发、屏障检查点、背压与恢复
cmd/demo        端到端演示：go run ./cmd/demo
```

## 1. 事件时间与水位线语义

- **事件时间**：每条 `event.Event` 携带 `Timestamp`（事件时间）与 `Seq`（数据源内唯一序号）。
  窗口归属只由事件时间决定：`Assigner` 按 Unix 纳秒对窗口长度向下取整，
  得到 `[start, end)` 区间，与到达顺序、处理时刻完全无关。
- **水位线**：`watermark = max(已观测事件时间) - MaxOutOfOrderness`，单调不减。
  语义为断言：“事件时间 ≤ 水位线的事件（在允许迟到范围外）已经到齐”。
- **窗口定稿**：窗口 `W` 的定稿线为 `W.End + AllowedLateness`。
  当水位线越过定稿线时，窗口被触发输出并清除状态。因此：
  - 乱序跨度 ≤ `MaxOutOfOrderness` 的事件永远被正常计入；
  - 事件时间 < 当前水位线、但其窗口尚未定稿（迟到 ≤ `AllowedLateness`）的事件**仍被计入**；
  - 窗口已定稿后到达的事件被**明确拒绝**，原因记为 `late_beyond_allowed_lateness`，
    可在 `Engine.Drops()` 明细与 `Stats.LateDropped` 计数中区分。
- **顺序无关性**：窗口聚合（求和/计数）与 Seq 去重都是可交换的；
  只要乱序落在 `MaxOutOfOrderness + AllowedLateness` 包络内，
  同一事件集合的任意到达排列产生完全相同的最终聚合（测试 `TestOutOfOrderAggregation`、
  `TestSameResultAcrossParallelism` 覆盖 1/2/4/8 分片 × 1/4/16 并发写入）。
- **同一 key 有序交付**：事件按 key 的 FNV 哈希路由到固定分片串行处理，
  同一 key 的窗口结果按窗口起始时间升序写出（`TestInOrderPerKeyDelivery`）。

## 2. 检查点、恢复与幂等规则

- **检查点内容**：`{ID, Watermark, MaxSeq, CommittedWindows}`。
  通过**屏障协议**拍摄：路由线程向所有分片发送屏障，各分片处理完屏障前的全部事件
  （含窗口触发写出）后暂停，此时快照满足不变式——
  **凡定稿线 ≤ 水位线的窗口都已在 `CommittedWindows` 中**。这保证恢复后不会静默错算。
- **恢复模型**：窗口中间态不持久化；恢复时加载最近检查点，恢复水位线与已提交窗口集合，
  数据源**从头重放**（位点回退可界定：无检查点则回退到起点）。
  三层幂等共同保证“不重不丢”：
  1. 引擎层：恢复出的 `CommittedWindows` 中的窗口重建后不再输出；
  2. 事件层：窗口状态按 `Seq` 去重，同一事件重复投递只计一次；
  3. 下游层：`sink` 以 `(key, windowStart, windowEnd)` 为幂等键，重复提交返回 `committed=false`。
- **崩溃语义**：`Abort()` 模拟进程崩溃（取消处理、丢弃在途缓冲、不做最终检查点）；
  `Close()` 是优雅停机（冲刷全部在途窗口并做最终检查点）。
  覆盖测试：`TestCrashReplayRecovery`（检查点恢复重放）、
  `TestRestartWithoutCheckpoint`（无检查点回退到起点 + 下游幂等）。

## 3. 背压策略与有界资源

- **有界缓冲**：入口通道 `in` 与每个分片通道容量均为 `InputBuffer`，
  在途事件上限 = `InputBuffer × (1 + Parallelism)`，与注入速率无关，内存不会无限增长。
- **非阻塞入口**：`Enqueue` 在缓冲满时**立即返回 false** 并记录
  `backpressure_buffer_full`（计数 `Stats.BackpressureDropped` + 有界明细样本），
  由调用方决定重试或丢弃；引擎自身绝不静默丢事件。
- **无死锁**：数据流向是单向管线 `in → router → shard → sink`，无环形等待；
  慢下游只会反压到入口使其拒绝新事件。`TestBackpressureDropsBounded` 验证
  300 次注入在容量 2 的缓冲下：接收 + 背压拒绝 ≡ 注入总数（事件守恒），
  且 `Close` 在慢下游下按时完成。
- **背压期间的正确性**：被接收的事件最终都会输出（运行中定稿或 Close 冲刷），
  被拒绝的事件全部可区分计数，二者不重叠。

## 4. 验证方法

```bash
go build ./...
go vet ./...
go test ./... -race -count=1     # 全部单测（含竞态检测）
go test ./pkg/engine/ -v -run TestLatenessBoundary   # 查看水位线/窗口/判定日志
go run ./cmd/demo                # 端到端演示
```

测试矩阵：

| 场景 | 测试 |
|---|---|
| 乱序（包络内）顺序无关 | `TestOutOfOrderAggregation`（3 种排列） |
| 迟到边界：范围内计入 / 范围外拒绝 | `TestLatenessBoundary` |
| 水位线运行中触发 | `TestWatermarkTriggerDuringRun` |
| 重复投递 = 单次投递 | `TestStateDedup`、`TestSameResultAcrossParallelism`（含重复注入） |
| 并发度无关性 | `TestSameResultAcrossParallelism`（分片 1/2/4/8 × 并发 1/4/16） |
| 检查点恢复 | `TestCrashReplayRecovery`、`TestRestartWithoutCheckpoint` |
| 背压有界 / 无死锁 / 可区分计数 | `TestBackpressureDropsBounded` |
| 按 key 有序交付 | `TestInOrderPerKeyDelivery` |
| 下游幂等 | `TestIdempotentWrite` |

单测日志中以 `[水位线判定]`、`[窗口判定]`、`[逐条判定]`、`[检查点]`、`[窗口结果]`、
`[引擎统计]`、`[拒绝明细]` 等前缀打印水位线、窗口边界与每条事件的判定依据。
