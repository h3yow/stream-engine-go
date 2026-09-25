package stream

import (
	"errors"
	"log"
	"math"
	"sync"
)

// ErrSimulatedCrash 由 Config.FailAfter 触发，模拟任务故障。
var ErrSimulatedCrash = errors.New("stream: simulated crash")

// Config 是引擎配置。
type Config struct {
	WindowSize      int64          // 窗口大小（毫秒）
	OutOfOrderness  int64          // 乱序容忍（毫秒），用于水位线推进
	AllowedLateness int64          // 允许迟到（毫秒），窗口关闭前的额外等待
	InboxCapacity   int            // 有界缓冲容量
	Policy          OverflowPolicy // 缓冲满时的策略
	// ProducerConcurrency 注入协程数：源按位点分片并发注入，
	// 每个分片是一个水位线分区。处理循环恒为单协程，
	// 且水位线取各分区最小值，因此聚合结果与并发度无关。<=0 视为 1。
	ProducerConcurrency int
	// CheckpointEvery 每处理多少条事件做一次检查点（<=0 表示不做）。
	CheckpointEvery int
	CheckpointStore CheckpointStore
	// FailAfter 处理满 N 条事件后返回 ErrSimulatedCrash（<=0 不启用），用于恢复测试。
	FailAfter int
	// Logger 可选：打印水位线、窗口与判定依据，便于测试与排查。
	Logger *log.Logger
}

// Engine 是本地事件流处理引擎。
// 处理循环为单协程：窗口状态与水位线只在此协程内变更，
// 因此语义与注入并发度、事件到达顺序无关。
type Engine struct {
	cfg    Config
	src    Source
	sink   Sink
	inbox  *Inbox
	wm     *WatermarkTracker
	win    *WindowManager
	stats  Counters
	mu     sync.Mutex // 保护 reject
	reject []Rejection
	nextCp int64
}

// New 创建引擎。
func New(cfg Config, src Source, sink Sink) *Engine {
	e := &Engine{cfg: cfg, src: src, sink: sink, nextCp: 1}
	e.wm = NewWatermarkTracker(cfg.OutOfOrderness, e.shards())
	e.win = NewWindowManager(cfg.WindowSize, cfg.AllowedLateness)
	e.inbox = NewInbox(cfg.InboxCapacity, cfg.Policy, &e.stats)
	return e
}

func (e *Engine) shards() int {
	if e.cfg.ProducerConcurrency <= 0 {
		return 1
	}
	return e.cfg.ProducerConcurrency
}

// Run 运行引擎直到源耗尽；若 CheckpointStore 中存在检查点则从中恢复。
// 恢复语义：从检查点记录的 NextOffset 继续读取，未触发窗口状态原样恢复，
// 已触发窗口的结果由 Sink 幂等键保证不重复输出。
func (e *Engine) Run() error {
	start := Offset(0)
	if e.cfg.CheckpointStore != nil {
		cp, ok, err := e.cfg.CheckpointStore.LoadLatest()
		if err != nil {
			return err
		}
		if ok {
			start = cp.NextOffset
			e.wm = NewWatermarkTracker(e.cfg.OutOfOrderness, e.shards())
			e.wm.restoreMaxSeen(cp.MaxSeen)
			e.wm.watermark = cp.Watermark
			e.win.Restore(cp.Windows)
			e.nextCp = cp.ID + 1
			e.logf("恢复检查点 id=%d nextOffset=%d watermark=%d 未触发窗口=%d",
				cp.ID, cp.NextOffset, cp.Watermark, len(cp.Windows))
		}
	}

	// 注入协程组：源按位点分片并发注入有界缓冲，分片即水位线分区。
	// 引擎消费不依赖注入完成，Block 策略下不会死锁。
	shards := e.shards()
	var wg sync.WaitGroup
	for s := 0; s < shards; s++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			for off := start + Offset(shard); ; off += Offset(shards) {
				ev, next, ok := e.src.Read(off)
				if !ok {
					e.inbox.Finish(shard)
					return
				}
				e.inbox.Offer(ev, next, shard)
			}
		}(s)
	}
	go func() { wg.Wait(); e.inbox.Close() }()

	// 连续位点跟踪：检查点只记录“已连续处理完成”的位点，
	// 保证位点回退可界定（回退范围 <= 一个检查点间隔），不产生静默错算。
	tracker := &offsetTracker{next: start, done: make(map[Offset]bool)}

	processed := 0
	for env := range e.inbox.C() {
		if env.Done {
			e.wm.Finish(env.Partition)
			e.logf("分区=%d 耗尽 水位线=%d", env.Partition, e.wm.Watermark())
			e.fireWindows(e.wm.Watermark())
			continue
		}
		e.process(env)
		processed++
		frontier := tracker.mark(env.Next)
		if e.cfg.FailAfter > 0 && processed >= e.cfg.FailAfter {
			return ErrSimulatedCrash
		}
		if e.cfg.CheckpointEvery > 0 && processed%e.cfg.CheckpointEvery == 0 {
			if err := e.saveCheckpoint(frontier); err != nil {
				return err
			}
		}
	}

	// 输入结束：水位线视为 +∞，触发所有剩余窗口。
	e.fireWindows(math.MaxInt64)
	if e.cfg.CheckpointStore != nil && e.cfg.CheckpointEvery > 0 {
		if err := e.saveCheckpoint(tracker.frontier()); err != nil {
			return err
		}
	}
	return nil
}

// offsetTracker 跟踪连续处理完成的源位点。
type offsetTracker struct {
	next Offset          // 下一个未确认的连续位点
	done map[Offset]bool // 已处理但尚未连续的位点
}

// mark 记录“位点 n-1 已处理”，返回当前连续推进到的位点。
func (t *offsetTracker) mark(n Offset) Offset {
	finished := n - 1
	if finished == t.next {
		t.next++
		for t.done[t.next] {
			delete(t.done, t.next)
			t.next++
		}
	} else {
		t.done[finished] = true
	}
	return t.next
}

func (t *offsetTracker) frontier() Offset { return t.next }

// process 处理单条事件：迟到判定 -> 计入窗口 -> 推进水位线 -> 触发窗口。
func (e *Engine) process(env Envelope) {
	ev := env.Ev
	e.stats.Received++
	wmBefore := e.wm.Watermark()
	accepted, reason := e.win.Add(ev, wmBefore)
	wmAfter := e.wm.Observe(env.Partition, ev.EventTime)

	switch {
	case accepted:
		e.stats.Accepted++
		e.logf("接受 事件=%s key=%s ts=%d 分区=%d 窗口=%v 水位线 %d->%d",
			ev.ID, ev.Key, ev.EventTime, env.Partition, e.win.Assign(ev.EventTime), wmBefore, wmAfter)
	case reason == RejectDuplicate:
		e.stats.Duplicates++
		e.recordRejection(ev, reason)
		e.logf("去重 事件=%s key=%s ts=%d 原因=%s（幂等：重复投递不计入）", ev.ID, ev.Key, ev.EventTime, reason)
	default:
		e.stats.Rejected++
		e.recordRejection(ev, reason)
		e.logf("拒绝 事件=%s key=%s ts=%d 原因=%s（水位线=%d 允许迟到=%d 下限=%d）",
			ev.ID, ev.Key, ev.EventTime, reason, wmBefore, e.cfg.AllowedLateness, wmBefore-e.cfg.AllowedLateness)
	}
	e.fireWindows(wmAfter)
}

// fireWindows 触发并输出当前水位线下所有可关闭窗口。
func (e *Engine) fireWindows(watermark int64) {
	for _, w := range e.win.Fireable(watermark) {
		for _, res := range e.win.Fire(w) {
			e.logf("触发 窗口=[%d,%d) key=%s sum=%d count=%d 水位线=%d",
				res.Window.Start, res.Window.End, res.Key, res.Sum, res.Count, watermark)
			if err := e.sink.Write(res); err != nil {
				e.logf("下游写出失败: %v", err)
				continue
			}
			e.stats.Emitted++
		}
	}
}

// saveCheckpoint 保存与处理进度一致的检查点。
func (e *Engine) saveCheckpoint(next Offset) error {
	cp := Checkpoint{
		ID:         e.nextCp,
		NextOffset: next,
		Watermark:  e.wm.Watermark(),
		MaxSeen:    e.wm.MaxSeen(),
		Windows:    e.win.Snapshot(),
	}
	if err := e.cfg.CheckpointStore.Save(cp); err != nil {
		return err
	}
	e.logf("检查点 id=%d nextOffset=%d watermark=%d 未触发窗口=%d",
		cp.ID, cp.NextOffset, cp.Watermark, len(cp.Windows))
	e.nextCp++
	return nil
}

func (e *Engine) recordRejection(ev Event, r RejectReason) {
	e.mu.Lock()
	e.reject = append(e.reject, Rejection{Event: ev, Reason: r})
	e.mu.Unlock()
}

func (e *Engine) logf(format string, args ...any) {
	if e.cfg.Logger != nil {
		e.cfg.Logger.Printf(format, args...)
	}
}

// Stats 返回运行计数快照。
func (e *Engine) Stats() Counters { return e.stats }

// Rejections 返回被拒绝/丢弃事件的记录（含可区分原因）。
func (e *Engine) Rejections() []Rejection {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Rejection, len(e.reject))
	copy(out, e.reject)
	return out
}
