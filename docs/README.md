# 本地事件流处理引擎

纯 Go 标准库实现（`module github.com/h3yow/stream-engine-go`，无第三方依赖）的本地事件流处理引擎：
本地模拟数据源注入事件，按**事件时间**做滚动窗口聚合，以**水位线**驱动窗口定稿，
支持**检查点恢复**与**有界背压**，结果写入幂等的本地模拟下游。
面向线上多分片数据源，进一步支持：**空闲分片水位线推进**（暂时空闲 vs 彻底终结）、
**扩缩容恢复**（换并行度从检查点继续跑，结果一致）与**运行期状态快照**（排障/验收）。

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

### 多分片数据源与空闲分片

- **分片跟踪**：每条事件可携带 `Partition`（数据源分片 ID，零值 0 表示单分片）。
  水位线按分片独立跟踪最大事件时间，全局水位线 = **活跃分片的最小贡献**：
  `wm = min over 活跃分片 (分片最大事件时间 - MaxOutOfOrderness)`，单调不减。
  任何一个活跃分片不吐数据，水位线就停在它被拖住的位置。
- **已知分片集合**：`Config.SourcePartitions > 0` 时引擎启动即登记全部分片；
  已登记但从未供数的分片会把水位线压在当前值，直到它首次供数或被标记空闲/终结，
  避免“分片还没被发现，水位线已越过它的数据”。`SourcePartitions = 0` 时按事件
  到达惰性发现分片（兼容旧行为）。
- **暂时空闲（idle）**：`MarkPartitionIdle(p)` 或配置 `PartitionIdleTimeout`
  （按处理时间自动判定沉默分片）后，该分片被**排除**在水位线最小值计算之外，
  水位线跟随其余活跃分片继续推进，该定稿的窗口照常定稿。
  空闲分片重新来数据时**自动恢复活跃**，其事件按正常规则处理：
  落在允许迟到范围内（窗口定稿线未到）正常计入、Seq 去重不重复；
  超出范围（窗口已定稿）明确拒绝，原因 `late_beyond_allowed_lateness`。
- **彻底终结（finished）**：`MarkPartitionFinished(p)` 表示分片永久不再供数。
  与 idle 的区别：finished 不可逆、不会因新事件复活；其后续事件一律以
  `partition_finished` 原因拒绝（独立计数 `Stats.PartitionFinishedDropped`，
  与迟到拒绝可区分），且这些事件不再影响水位线。
- **全部空闲**：所有分片都空闲/终结时，水位线停在历史最高值（floor），
  不推进也不回退；窗口最终由 `Close()` 冲刷定稿。
- **跨处理分片的定稿传播**：水位线推进时，处理分片之间会互相发送推进通知，
  因此即使某处理分片长期没有新事件（其 key 都映射到空闲数据源分片），
  它上面已到定稿线的窗口也会被及时输出，而不是等到关停冲刷。
- 覆盖测试：`TestIdlePartitionWatermarkAdvance`（空闲→推进→恢复→范围内计入/
  超范围拒绝）、`TestFinishedPartitionRejected`（idle 与 finished 的区别）、
  `TestPartitionIdleTimeout`（超时自动空闲）、`TestPartitionStateRestored`
  （idle/finished 标记随检查点恢复）、`pkg/watermark` 的分片单测。

## 2. 检查点、恢复与幂等规则

- **检查点内容**：`{ID, Version, Parallelism, Watermark, MaxSeq, CommittedWindows,
  Partitions, CreatedAt}`。
  通过**屏障协议**拍摄：路由线程向所有分片发送屏障，各分片处理完屏障前的全部事件
  （含窗口触发写出）后暂停，此时快照满足不变式——
  **凡定稿线 ≤ 水位线的窗口都已在 `CommittedWindows` 中**。这保证恢复后不会静默错算。
- **恢复模型**：窗口中间态不持久化；恢复时加载最近检查点，恢复水位线（含各数据源
  分片的 idle/finished 状态）与已提交窗口集合，数据源**从头重放**
  （位点回退可界定：无检查点则回退到起点）。
  三层幂等共同保证“不重不丢”：
  1. 引擎层：恢复出的 `CommittedWindows` 中的窗口重建后不再输出；
  2. 事件层：窗口状态按 `Seq` 去重，同一事件重复投递只计一次；
  3. 下游层：`sink` 以 `(key, windowStart, windowEnd)` 为幂等键，重复提交返回 `committed=false`。
- **崩溃语义**：`Abort()` 模拟进程崩溃（取消处理、丢弃在途缓冲、不做最终检查点）；
  `Close()` 是优雅停机（冲刷全部在途窗口并做最终检查点）。
  覆盖测试：`TestCrashReplayRecovery`（检查点恢复重放）、
  `TestRestartWithoutCheckpoint`（无检查点回退到起点 + 下游幂等）。

### 扩缩容恢复（换并行度继续跑）

- **兼容范围**：检查点中的全部状态（全局水位线、各数据源分片水位线状态、
  已提交窗口集合、MaxSeq）都是**并行度无关**的全局状态——已提交窗口按
  `(key, windowStart)` 标识而非按处理分片标识，窗口中间态本就靠重放重建。
  因此从 `Parallelism = P` 的检查点以 `Parallelism = Q` 恢复是安全的，
  最终结果与同数据在 Q 下从头跑出的一致（`TestRescaleRecovery` 覆盖
  扩容 2→4、缩容 4→2、同构 2→2；`TestRescaleResultEqualsAnyParallelism`
  交叉对比并行度 1/2/4 与 2→4 恢复的结果）。
- **恢复情况核对**：`Engine.RestoreInfo()`（也包含在状态快照中）报告
  是否恢复、检查点 ID、原/新并行度、是否发生扩缩容、恢复出的已提交窗口数。
- **不能恢复的情形（明确报错，不悄悄算错）**：`Start` 返回
  `ErrIncompatibleCheckpoint`——
  1. 检查点 `Version` 高于当前引擎支持的版本；
  2. 旧格式检查点（`Parallelism <= 0`，未记录并行度元数据）：
     无法验证其状态是否并行度无关，拒绝用于恢复。
  出现该错误时需显式处理：删除检查点全新启动，或用与检查点匹配的版本/配置恢复。
  覆盖测试：`TestIncompatibleCheckpointRejected`。

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

## 4. 运行期状态快照

`Engine.Snapshot(ctx)` 在屏障点拍摄一份一致性快照（与事件处理串行，快照内部自洽），
供排障与验收使用。字段含义稳定：

| 字段 | 含义 | 验证方法 |
|---|---|---|
| `Watermark` | 快照时刻的全局事件时间水位线 | 与 `Engine.Watermark()` 一致；定稿条件 `wm >= 窗口End + AllowedLateness` |
| `Parallelism` | 当前处理分片数 | 等于 `Config.Parallelism` |
| `Partitions[].ID` | 数据源分片 ID | 与事件的 `Partition` 字段对应 |
| `Partitions[].Status` | `active` / `idle` / `finished` | idle 可由 `MarkPartitionIdle` 或空闲超时触发；finished 只能显式标记且不可逆 |
| `Partitions[].MaxObserved` | 该分片已观测的最大事件时间 | 对照注入的该分片事件时间最大值 |
| `Partitions[].HasData` | 该分片是否供过数 | false 的活跃分片会把水位线压在当前值 |
| `Partitions[].Watermark` | 该分片的水位线贡献（`MaxObserved - MaxOutOfOrderness`） | 全局水位线 = 活跃分片该值的最小者 |
| `PendingWindows[]` | 仍在等待定稿的窗口（key、区间、当前 sum/count、定稿线） | 定稿线 = `End + AllowedLateness`；水位线越过即转入已提交 |
| `CommittedWindows` | 已定稿提交的窗口总数（含检查点恢复出的） | 与下游实际写出条数互证 |
| `Drops` | 被拒事件按原因的精确计数 | 三种原因：`late_beyond_allowed_lateness`、`backpressure_buffer_full`、`partition_finished` |
| `Stats` | 引擎累计计数 | 同 `Engine.Stats()` |
| `Restore` | 启动时的检查点恢复情况 | 同 `Engine.RestoreInfo()` |

另：`Engine.DropCounts()` 可随时单独获取按原因的拒绝精确计数（不受明细样本
上限 1024 影响）；`Engine.Drops()` 返回被拒绝事件的有界明细样本。
覆盖测试：`TestStatusSnapshot`（逐字段校验 + 与下游输出互证）、
`TestSnapshotRestoreField`（快照中的恢复信息）。

## 5. 验证方法

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
| 空闲分片水位线推进 / 恢复后迟到判定 | `TestIdlePartitionWatermarkAdvance`、`TestPartitionIdleTimeout` |
| 暂时空闲 vs 彻底终结 | `TestFinishedPartitionRejected`、`TestPartitionStateRestored` |
| 分片水位线单测 | `pkg/watermark`：空闲排除、终结不可逆、全空闲 floor、超时清扫、分片恢复 |
| 扩缩容恢复（2→4 / 4→2 / 2→2） | `TestRescaleRecovery`、`TestRescaleResultEqualsAnyParallelism` |
| 不兼容检查点明确拒绝 | `TestIncompatibleCheckpointRejected` |
| 状态快照字段 | `TestStatusSnapshot`、`TestSnapshotRestoreField` |

单测日志中以 `[水位线判定]`、`[窗口判定]`、`[逐条判定]`、`[检查点]`、`[窗口结果]`、
`[引擎统计]`、`[拒绝明细]`、`[快照]`、`[恢复]`、`[扩缩容核对]` 等前缀打印水位线、
窗口边界与每条事件的判定依据。
