# stream-engine-go

本地事件流处理引擎：用本地模拟的数据源（`MemSource`）与下游写出（`MemSink`），
实现事件时间窗口聚合、水位线推进、检查点恢复与背压控制，
保证在**声明的乱序上界内**，结果与到达顺序、注入并发度无关。

代码位于 `stream/` 包，仅使用 Go 标准库。

## 事件时间与水位线语义

- 每条事件携带事件时间 `EventTime`（毫秒），按 `WindowSize` 分配滚动窗口 `[start, end)`。
- 水位线 = **各注入分区最大事件时间的最小值** − `OutOfOrderness`，只增不减。
  - 多注入协程（`ProducerConcurrency`）时每个分片是一个水位线分区，
    慢分区拖住水位线，快分区的进度不会误伤慢分区的迟到事件——
    这是结果与并发度无关的关键。
  - 分区耗尽时发送控制消息将其移出最小值计算。
- 迟到判定（与事件到达时的当前水位线比较）：
  - `eventTime > watermark`：正常事件；
  - `watermark - AllowedLateness < eventTime <= watermark`：允许范围内的迟到，正常计入；
  - `eventTime <= watermark - AllowedLateness`：超出允许范围，**明确拒绝**，
    记录原因 `late_beyond_allowed`（可通过 `Engine.Rejections()` 查询）。
- 窗口在水位线越过 `window.End + AllowedLateness` 后触发并关闭；
  输入结束时水位线视为 +∞，触发所有剩余窗口。

**确定性契约**：当实际乱序幅度（事件时间口径）小于
`OutOfOrderness + AllowedLateness` 时，同一事件集合的聚合结果与到达顺序、
注入并发度、缓冲容量完全无关。超出声明上界的乱序不属于静默错算——
引擎会拒绝并给出可区分计数（`Counters.Rejected`）。

## 恢复与幂等规则

- 检查点内容：连续源位点 `NextOffset`、水位线、各分区最大事件时间、
  未触发窗口的完整状态（含已计入事件 ID 集合）。
  文件存储（`FileCheckpointStore`）写临时文件后原子改名，避免半截检查点。
- **位点一致性**：注入位点随事件流经缓冲，消费端用 `offsetTracker`
  维护“已连续处理完成”的位点，检查点只记录连续位点。
  因此回退范围可界定：`(检查点连续位点, 故障点]`，不超过一个检查点间隔
  （或回退到可用的最近检查点）。
- **恢复语义**：从检查点恢复窗口状态与水位线，从 `NextOffset` 续读。
  检查点之后、故障之前处理过的事件会被重放，但其状态已随崩溃回滚，
  属于“首次计入”，不会重复计数。
- **幂等**：
  - 窗口内按事件 ID 去重：同一事件重复投递与只投递一次结果一致
    （重复投递计入 `Counters.Duplicates`，原因 `duplicate`）；
  - 下游按 `(窗口, Key)` 幂等键去重：恢复导致已触发窗口重算重发时，
    `MemSink` 不会追加重复记录。

## 背压策略

- 源与引擎之间是固定容量的有界缓冲（`InboxCapacity`），内存占用有界。
- `Block`（默认）：缓冲满时注入方阻塞，背压传导到上游；
  阻塞次数计入 `Counters.Blocked`。消费循环不依赖注入方完成，不会死锁。
- `DropNewest`：缓冲满时丢弃新事件，计入 `Counters.Dropped`，
  原因 `dropped_backpressure` 可区分；守恒式 `接收 + 丢弃 = 注入` 保证无静默丢失。
- 分区结束等控制消息恒被送达，不参与丢弃。

## 可观测性

`Engine.Stats()` 返回计数快照，满足守恒：
`Received = Accepted + Duplicates + Rejected`（另计 `Dropped/Blocked`）。
配置 `Config.Logger` 后，引擎逐条打印：事件接受/去重/拒绝及判定依据
（当前水位线、允许迟到下限）、水位线推进、窗口触发与检查点。

## 验证方法

```bash
go test ./... -v        # 全部测试，日志含水位线/窗口/判定依据
go test ./... -race     # 并发竞态检测
```

测试覆盖（`stream/` 下）：

| 场景 | 测试 |
| --- | --- |
| 水位线单调性、窗口分配、迟到分类 | `watermark_window_test.go` |
| 有界乱序下顺序无关 | `TestOrderIndependence` |
| 并发度/缓冲容量无关 | `TestConcurrencyIndependence` |
| 重复投递幂等 | `TestDuplicateDelivery` |
| 迟到端到端（范围内计入/范围外拒绝） | `TestLateEventsEndToEnd` |
| 超界乱序明确拒绝 | `TestBeyondBoundRejection` |
| 计数守恒 | `TestNoSilentMiscount` |
| 崩溃恢复不重复不丢失 | `TestCrashRecovery` |
| 位点回退有界 | `TestRollbackBounded` |
| 从较早检查点恢复 | `TestRecoverFromEarlierCheckpoint` |
| 恢复时变更并发度 | `TestRecoveryWithDifferentConcurrency` |
| 背压 Block/Drop、有界内存、不死锁 | `backpressure_test.go` |
