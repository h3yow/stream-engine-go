# 本地事件流处理引擎

纯 Go 标准库实现（`module github.com/h3yow/stream-engine-go`，无第三方依赖）的本地事件流处理引擎：
本地模拟数据源注入事件，按**事件时间**做滚动窗口聚合，以**水位线**驱动窗口定稿，
支持**检查点恢复**与**有界背压**，结果写入幂等的本地模拟下游。

```
pkg/event       事件类型与可区分的丢弃原因
pkg/watermark   有界乱序水位线生成器 + 分片水位线跟踪器（空闲检测/终止宣告）
pkg/window      滚动窗口分配与聚合状态（含 Seq 去重）
pkg/checkpoint  检查点结构与存储（内存实现）
pkg/sink        幂等收集型下游（可注入写延迟模拟慢下游）
pkg/source      确定性事件重放数据源
pkg/engine      编排：路由分片、水位线触发、屏障检查点、背压、恢复与状态快照
cmd/demo        端到端演示：go run ./cmd/demo
```

## 1. 事件时间与水位线语义

- **事件时间**：每条 `event.Event` 携带 `Timestamp`（事件时间）与 `Seq`（数据源内唯一序号）。
  窗口归属只由事件时间决定：`Assigner` 按 Unix 纳秒对窗口长度向下取整，
  得到 `[start, end)` 区间，与到达顺序、处理时刻完全无关。
- **水位线**：单分片时 `watermark = max(已观测事件时间) - MaxOutOfOrderness`，单调不减；
  多分片时按分片分别跟踪再取 min（见下文“多分片数据源的水位线”）。
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

### 多分片数据源的水位线

数据源通常是多分片的（`Event.Partition` 标识来源分片，空串为未分片数据源的默认分片）。
`watermark.Tracker` 按分片分别跟踪已观测最大事件时间，合成全局水位线：

- **合成规则**：`全局水位线 = min(各 active 分片水位线)`，且**单调不减**。
  单调性优先于 min：晚注册的分片（其首条事件处理前，全局水位线已被其他分片推进）
  不会把全局水位线拉回——窗口定稿不可逆。
- **空闲（idle）**：某分片超过 `Config.IdleTimeout`（处理时间）未供数即被判定 idle，
  **暂时**从 min 计算中排除，全局水位线随其余正常供数的分片继续推进，
  到期窗口照常定稿（水位线推进由分片→路由→全分片的冲刷广播传播，
  空闲分片自己的 key 所在分片也能及时定稿）。该分片恢复供数后自动回到 active：
  落在允许迟到范围内的事件正常计入（重复投递仍按 Seq 去重），
  超出范围的按 `late_beyond_allowed_lateness` 明确拒绝。
- **终止（finished）**：`Engine.FinishPartition(id)` 显式宣告某分片**彻底不再供数**，
  与 idle 的”暂时没数据、可自动恢复”不同：finished 不可逆，分片被永久排除，
  其后续事件一律以 `partition_finished` 原因拒绝（计数 `Stats.PartitionFinishedDropped`）。
  宣告时刻会记录**终止位点**（该分片已观测的最大事件序号 `FinishSeq`），
  用于检查点恢复后的重放判定（见下文”恢复前后的 idle/finished 语义”）。
- **全部空闲**：所有分片都 idle/finished 时，全局水位线停在最近值不再推进。
- 覆盖测试：`TestTracker*`（跟踪器单测，注入时钟）、
  `TestIdlePartitionWatermarkAdvances`（空闲→推进→恢复计入/去重/拒绝）、
  `TestFinishedPartitionRejected`（终止与空闲的不同表现）。

### 恢复前后的 idle/finished 语义

两种状态都会随检查点持久化（`Checkpoint.Partitions` 记录每个分片的
`MaxObserved / Finished / FinishSeq`），恢复后一直分得开、表现不一样：

- **idle（只是暂时没数据）**：恢复后以 active 重新开始空闲计时；静默超过
  `IdleTimeout` 照常转 idle、被排除出 min 计算，全局水位线跟着其他还在供数的
  分片往前走、该定稿的窗口照常定稿；它回来供数时，落在允许迟到范围内的事件
  正常计入、重复投递按 Seq 去重不重复算、明显超范围的老事件照样以
  `late_beyond_allowed_lateness` 拒绝——与恢复前完全一致。
- **finished（已经彻底不再供数）**：恢复后**不变回正常供数**，永久排除出水位线
  计算；这些分片上”再来的事件”（序号越过终止位点）仍以 `partition_finished`
  明确拒绝，与迟到/背压可区分，不会被重新算进窗口。**一条数据都没出过就被停掉
  的分片**同样登记在检查点中（`Finished=true`、无终止位点），恢复后其任何事件
  都被拒绝，不会因为没有历史数据而被漏掉。
- **终止位点的作用**：窗口中间态不持久化，恢复靠数据源从头重放。终止前已计入、
  但检查点时尚未定稿的窗口，其事件（序号 ≤ `FinishSeq`）在重放时必须被**重新
  接受**以完整重建窗口——否则恢复后会少算；序号越过位点的事件（终止后到达的）
  一律拒绝——否则恢复后会多算。由此保证恢复前后最终聚合完全一致。
- 覆盖测试：`TestFinishedPartitionSurvivesRecovery`（并行度 1→1/2→2/2→4/4→1，
  含从未供数即终止的分片）、`TestIdleVsFinishedAfterRecovery`（并行度 1/2/4，
  同一运行中 idle 与 finished 恢复后表现可分）、
  `TestTrackerFinishCut` / `TestTrackerRestoreFinished`（跟踪器单测）。

## 2. 检查点、恢复与幂等规则

- **检查点内容**：`{ID, Watermark, MaxSeq, CommittedWindows, Parallelism,
  WindowSize, MaxOutOfOrderness, AllowedLateness, Partitions}`，
  其中 `Partitions` 记录每个数据源分片的 `{MaxObserved, Finished, FinishSeq}`。
  通过**两阶段对齐协议**拍摄：路由线程先向所有分片发对齐标记（各分片处理完
  标记前的全部事件后 ack），在静态点读取割切水位线，再发冲刷标记让各分片
  **按同一割切水位线**补齐到期窗口。此时快照满足不变式——
  **凡定稿线 ≤ 水位线的窗口都已在 `CommittedWindows` 中**。这保证恢复后不会静默错算。
  （若让各分片各自读取水位线，先读的分片用旧值、后读的用新值，
  同一割切下窗口定稿口径不一，快照会自相矛盾。）
- **恢复模型**：窗口中间态不持久化；恢复时加载最近检查点，恢复水位线、各分片
  供数状态（含终止标记与终止位点）与已提交窗口集合，数据源**从头重放**
  （位点回退可界定：无检查点则回退到起点）。
  三层幂等共同保证”不重不丢”：
  1. 引擎层：恢复出的 `CommittedWindows` 中的窗口重建后不再输出；
  2. 事件层：窗口状态按 `Seq` 去重，同一事件重复投递只计一次；
  3. 下游层：`sink` 以 `(key, windowStart, windowEnd)` 为幂等键，重复提交返回 `committed=false`。
- **崩溃语义**：`Abort()` 模拟进程崩溃（取消处理、丢弃在途缓冲、不做最终检查点）；
  `Close()` 是优雅停机（冲刷全部在途窗口并做最终检查点）。
  覆盖测试：`TestCrashReplayRecovery`（检查点恢复重放）、
  `TestRestartWithoutCheckpoint`（无检查点回退到起点 + 下游幂等）。

### 扩缩容恢复（换并行度）

- **兼容范围**：检查点中的 `CommittedWindows`、水位线、各分片供数状态
  （含 finished 标记与终止位点）都是**并行度无关**的状态（窗口按 key 标识、
  水位线按数据源分片合成），因此允许以不同 `Parallelism`
  恢复——扩容（如 2→4）与缩容（如 4→1）均可。恢复后已提交窗口不重复输出、
  未提交窗口不丢失、已终止分片不复活，最终聚合与原并行度下的参考运行完全一致
  （`TestRescaleRecovery`：2→4 / 4→1 / 2→2，断言下游零重复跳过、写出数=窗口数；
  `TestFinishedPartitionSurvivesRecovery`：带终止分片的 1→1/2→2/2→4/4→1）。
- **可兼容的旧状态**：只要三个窗口语义参数一致，下列检查点都可安全恢复——
  任意并行度拍摄的、含或不含分片信息的、分片处于任意供数状态
  （active/idle/finished）的。分片信息缺失（空 map）时仅由快照水位线兜底，
  水位线不回退，但终止分片信息也随之丢失——因此需要保留终止语义的部署
  应使用记录 `Partitions` 的当前格式检查点。
- **不兼容判定**：`WindowSize`、`MaxOutOfOrderness`、`AllowedLateness` 是窗口语义参数，
  决定窗口边界与定稿判定。检查点记录的这三个参数与引擎当前配置不一致时，
  `Start` 返回 `ErrIncompatibleCheckpoint`（错误信息含双方参数明细），
  **明确拒绝恢复而不是静默算错**。旧版本格式（未记录语义参数，零值）的检查点
  同样按不兼容拒绝。覆盖测试：`TestIncompatibleCheckpoint`。
- 检查点中的 `Parallelism` 字段仅记录拍摄时的并行度，供审计与排障，不参与兼容性判定。

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

`Engine.Snapshot()` 可在运行中随时调用。运行期间拍摄走与检查点相同的
**两阶段对齐协议**（路由线程串行执行，期间没有新事件进入分片）：
先对齐所有分片的处理进度，读取割切水位线，再让各分片按同一水位线补齐
到期窗口，最后读取全部字段。因此每份快照都是**同一时刻的一致视图**——
各字段对应同一处理割切，不会自相矛盾（例如不会出现"拒绝计数已加上、
对应窗口却还没出现"，或"窗口定稿线不晚于水位线却仍在待定清单"）；
并发处理的同时反复取快照，读到的结果也稳定、可直接核对
（`TestSnapshotConsistentUnderConcurrency`：并行度 1/2/4 × 并发注入 ×
并发反复取快照逐份校验）。引擎未启动或已关闭时处理管线静止，直接读取即是一致视图。

| 字段 | 含义 | 验证方法 |
|---|---|---|
| `Watermark` | 割切时刻的全局事件时间水位线；零值表示尚未形成 | 与 `Engine.Watermark()` 一致；应单调不减 |
| `MaxSeq` | 已处理事件的最大序号 | 应等于已注入事件的最大 Seq |
| `Partitions` | 各数据源分片状态：`ID`、`Status`（active/idle/finished）、`MaxObserved`（分片最大事件时间）、`Watermark`（分片水位线=MaxObserved-乱序跨度）、`LastEventAt`、`FinishSeq`/`HasFinishSeq`（终止位点，仅 finished 有意义） | 全局水位线不小于各 active 分片水位线的最小值（单调性优先）；超 `IdleTimeout` 未供数的分片应为 idle；已终止分片在任何快照中都应保持 finished |
| `PendingWindows` | 已打开但未定稿的窗口清单（等待水位线越过定稿线） | 与 `CommittedWindows` 不重叠；运行期快照中其定稿线（End+AllowedLateness）应都 > 快照水位线 |
| `CommittedWindows` | 已定稿并提交下游的窗口清单（含检查点恢复出的部分） | 与 `PendingWindows` 不重叠；运行期快照中其定稿线应都 ≤ 快照水位线；与下游输出一一对应（Close 冲刷的窗口不受定稿线约束，关闭后快照不适用此两条） |
| `Drops` | 被拒绝事件按原因的精确计数；键集合固定为 `late_beyond_allowed_lateness` / `backpressure_buffer_full` / `partition_finished` | 与 `Stats` 中对应计数一致 |
| `Stats` | 引擎累计计数（接收/计入/去重/各类拒绝/窗口输出/检查点数） | 事件守恒：接收 = 计入 + 重复跳过 + 迟到拒绝 + 终止拒绝（背压拒绝发生在入口、不进入处理管线，单独计数） |

覆盖测试：`TestSnapshotFields`（运行中/空闲后/关闭后三个时点逐项校验字段语义）、
`TestSnapshotConsistentUnderConcurrency`（并发下反复取快照的一致性）。

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
| 分片空闲：水位线继续推进、恢复后计入/去重/拒绝 | `TestIdlePartitionWatermarkAdvances`、`TestTrackerIdleExcluded` |
| 分片终止：与空闲可区分、后续事件拒绝 | `TestFinishedPartitionRejected`、`TestTrackerFinish`、`TestTrackerFinishCut` |
| 终止分片恢复后不复活（含扩缩容、含从未供数即终止） | `TestFinishedPartitionSurvivesRecovery`（1→1/2→2/2→4/4→1）、`TestTrackerRestoreFinished` |
| 恢复后 idle/finished 表现可分 | `TestIdleVsFinishedAfterRecovery`（并行度 1/2/4） |
| 扩缩容恢复（2→4 / 4→1 / 2→2）结果一致 | `TestRescaleRecovery` |
| 不兼容检查点明确拒绝 | `TestIncompatibleCheckpoint` |
| 状态快照字段语义 | `TestSnapshotFields` |
| 并发下反复取快照的一致性 | `TestSnapshotConsistentUnderConcurrency`（并行度 1/2/4） |
| 检查点恢复 | `TestCrashReplayRecovery`、`TestRestartWithoutCheckpoint` |
| 背压有界 / 无死锁 / 可区分计数 | `TestBackpressureDropsBounded` |
| 按 key 有序交付 | `TestInOrderPerKeyDelivery` |
| 下游幂等 | `TestIdempotentWrite` |

单测日志中以 `[水位线判定]`、`[窗口判定]`、`[逐条判定]`、`[检查点]`、`[窗口结果]`、
`[引擎统计]`、`[拒绝明细]`、`[空闲判定]`、`[终止判定]`、`[终止位点]`、`[恢复终止]`、
`[终止宣告]`、`[状态可分]`、`[扩缩容恢复]`、`[快照:*]`、`[快照一致性:*]`
等前缀打印关键时刻的水位线、涉及窗口与每条事件的判定依据，可直接核对。
