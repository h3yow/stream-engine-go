// Package engine 把数据源、事件时间窗口、水位线、检查点与下游编排为完整引擎。
package engine

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/checkpoint"
	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/sink"
	"github.com/h3yow/stream-engine-go/pkg/watermark"
	"github.com/h3yow/stream-engine-go/pkg/window"
)

// maxDropRecords 限制保留的丢弃明细条数（计数始终精确，明细仅保留有界样本）。
const maxDropRecords = 1024

// ErrNotRunning 表示在引擎未启动或已关闭后调用了需要运行态的方法。
var ErrNotRunning = errors.New("engine not running")

// Config 为引擎配置。
type Config struct {
	WindowSize        time.Duration // 滚动窗口长度（事件时间）
	MaxOutOfOrderness time.Duration // 水位线允许的最大乱序跨度
	AllowedLateness   time.Duration // 水位线越过窗口结束后，额外接受迟到事件的时长
	InputBuffer       int           // 入口有界缓冲容量（背压边界）
	Parallelism       int           // 处理分片数；同一 key 始终由同一分片串行处理
	CheckpointEveryN  int           // 每接收 N 条事件做一次周期检查点；0 表示关闭
}

// Stats 为引擎可观测计数。
type Stats struct {
	Received            int64
	Accepted            int64
	DuplicateSkipped    int64
	LateDropped         int64
	BackpressureDropped int64
	WindowsEmitted      int64
	Checkpoints         int64
}

// shardMsg 为路由到分片的消息：要么是一条事件，要么是检查点屏障。
type shardMsg struct {
	ev      event.Event
	barrier *barrier
}

// barrier 是分片间的暂停/恢复屏障，用于拍摄一致检查点。
type barrier struct {
	ack    chan struct{} // 每个分片处理到屏障时发送一次
	resume chan struct{} // 关闭后分片继续运行
}

type shard struct {
	ch     chan shardMsg
	states map[string]map[window.Window]*window.State
}

// Engine 是流处理引擎实例。
type Engine struct {
	cfg      Config
	snk      sink.Sink
	store    checkpoint.Store
	assigner *window.Assigner
	wm       *watermark.Generator

	in   chan event.Event
	quit chan struct{}

	shards    []*shard
	routerWg  sync.WaitGroup
	shardWg   sync.WaitGroup
	closeOnce sync.Once
	abortOnce sync.Once
	cancel    context.CancelFunc
	sendMu    sync.RWMutex // 保证 Close 时没有并发 Enqueue 向 in 发送
	closed    atomic.Bool
	started   atomic.Bool

	ckptReq chan ckptRequest
	ctx     context.Context

	// 原子计数器
	received            atomic.Int64
	accepted            atomic.Int64
	duplicateSkipped    atomic.Int64
	lateDropped         atomic.Int64
	backpressureDropped atomic.Int64
	windowsEmitted      atomic.Int64
	checkpoints         atomic.Int64
	maxSeq              atomic.Int64

	dropsMu sync.Mutex
	drops   []event.DropRecord

	committedMu sync.Mutex
	committed   map[checkpoint.WindowRef]struct{}
}

type ckptRequest struct {
	reply chan ckptResult
}

type ckptResult struct {
	cp  *checkpoint.Checkpoint
	err error
}

// New 创建引擎；store 为 nil 时不做持久化检查点。
func New(cfg Config, snk sink.Sink, store checkpoint.Store) *Engine {
	if cfg.WindowSize <= 0 {
		panic("engine: WindowSize must be positive")
	}
	if cfg.MaxOutOfOrderness < 0 {
		panic("engine: MaxOutOfOrderness must be non-negative")
	}
	if cfg.AllowedLateness < 0 {
		panic("engine: AllowedLateness must be non-negative")
	}
	if cfg.Parallelism < 1 {
		cfg.Parallelism = 1
	}
	if cfg.InputBuffer < 1 {
		cfg.InputBuffer = 1
	}
	return &Engine{
		cfg:       cfg,
		snk:       snk,
		store:     store,
		assigner:  window.NewTumblingAssigner(cfg.WindowSize),
		wm:        watermark.New(cfg.MaxOutOfOrderness),
		in:        make(chan event.Event, cfg.InputBuffer),
		quit:      make(chan struct{}),
		committed: make(map[checkpoint.WindowRef]struct{}),
		ckptReq:   make(chan ckptRequest),
	}
}

// Start 启动后台处理 goroutine 并从检查点恢复（若存在）。
//
// 恢复语义：加载最近检查点，恢复水位线与已提交窗口集合；
// 窗口中间态不持久化，要求数据源从最早事件开始重放，
// 由 Seq 去重、已提交窗口集合与下游幂等写共同保证结果不重不丢。
func (e *Engine) Start(ctx context.Context) error {
	if !e.started.CompareAndSwap(false, true) {
		return errors.New("engine: already started")
	}
	e.ctx, e.cancel = context.WithCancel(ctx)
	if e.store != nil {
		cp, err := e.store.LoadLatest(ctx)
		if err != nil {
			return err
		}
		if cp != nil {
			e.wm.Restore(cp.Watermark)
			for _, ref := range cp.CommittedWindows {
				e.committed[ref] = struct{}{}
			}
			e.maxSeq.Store(cp.MaxSeq)
		}
	}

	e.shards = make([]*shard, e.cfg.Parallelism)
	for i := range e.shards {
		sh := &shard{
			ch:     make(chan shardMsg, e.cfg.InputBuffer),
			states: make(map[string]map[window.Window]*window.State),
		}
		e.shards[i] = sh
		e.shardWg.Add(1)
		go e.runShard(e.ctx, sh)
	}
	e.routerWg.Add(1)
	go e.runRouter(e.ctx)
	return nil
}

// runRouter 单线程路由事件并串行化所有检查点请求。
func (e *Engine) runRouter(ctx context.Context) {
	defer e.routerWg.Done()
	var routed int64
	in := e.in
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-e.ckptReq:
			cp, err := e.barrierCheckpoint(ctx)
			req.reply <- ckptResult{cp: cp, err: err}
		case ev, ok := <-in:
			if !ok {
				// 入口关闭：关闭各分片通道，分片会冲刷剩余窗口后退出。
				for _, sh := range e.shards {
					close(sh.ch)
				}
				e.shardWg.Wait()
				// 冲刷完成后做最后一次检查点，固化全部已提交窗口。
				if e.store != nil {
					_, _ = e.saveSnapshot(ctx)
				}
				return
			}
			idx := shardIndex(ev.Key, len(e.shards))
			select {
			case e.shards[idx].ch <- shardMsg{ev: ev}:
			case <-ctx.Done():
				return
			}
			routed++
			if e.cfg.CheckpointEveryN > 0 && e.store != nil &&
				routed%int64(e.cfg.CheckpointEveryN) == 0 {
				_, _ = e.barrierCheckpoint(ctx)
			}
		}
	}
}

// runShard 串行处理属于本分片 key 的全部事件，因此该 key 上无需加锁。
func (e *Engine) runShard(ctx context.Context, sh *shard) {
	defer e.shardWg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sh.ch:
			if !ok {
				// 通道关闭：定稿冲刷，所有仍打开的窗口无条件输出。
				e.flushShard(sh)
				return
			}
			if msg.barrier != nil {
				// 处理到屏障：此前所有事件均已完成（含窗口触发写出）。
				msg.barrier.ack <- struct{}{}
				select {
				case <-msg.barrier.resume:
				case <-ctx.Done():
					return
				}
				continue
			}
			e.process(ctx, sh, msg.ev)
		}
	}
}

// process 处理单条事件：更新水位线、判定迟到、去重计入、触发到期窗口。
func (e *Engine) process(ctx context.Context, sh *shard, ev event.Event) {
	e.received.Add(1)
	if s := ev.Seq; s > e.maxSeq.Load() {
		e.maxSeq.Store(s)
	}

	wm := e.wm.Observe(ev.Timestamp)
	w := e.assigner.Assign(ev.Timestamp)
	// 窗口定稿时刻：水位线越过 End+AllowedLateness。
	deadline := w.End.Add(e.cfg.AllowedLateness)

	if !wm.Before(deadline) {
		// 窗口已定稿（状态已清除）：明确拒绝，原因可区分。
		e.reject(ev, event.ReasonLateBeyondAllowed)
		e.lateDropped.Add(1)
		e.emitDue(sh, wm)
		return
	}

	byKey, ok := sh.states[ev.Key]
	if !ok {
		byKey = make(map[window.Window]*window.State)
		sh.states[ev.Key] = byKey
	}
	st, ok := byKey[w]
	if !ok {
		st = window.NewState()
		byKey[w] = st
	}
	if st.Add(ev) {
		e.accepted.Add(1)
	} else {
		e.duplicateSkipped.Add(1)
	}
	e.emitDue(sh, wm)
}

// emitDue 输出本分片内 deadline <= wm 的全部窗口（按 key、起始时间有序）。
func (e *Engine) emitDue(sh *shard, wm time.Time) {
	type dueRef struct {
		key string
		w   window.Window
		st  *window.State
	}
	var due []dueRef
	for key, byKey := range sh.states {
		for w, st := range byKey {
			if !wm.Before(w.End.Add(e.cfg.AllowedLateness)) {
				due = append(due, dueRef{key: key, w: w, st: st})
			}
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].key != due[j].key {
			return due[i].key < due[j].key
		}
		return due[i].w.Start.Before(due[j].w.Start)
	})
	for _, d := range due {
		e.commitWindow(sh, d.key, d.w, d.st)
	}
}

// flushShad 输出分片内所有窗口（关闭时调用）。
func (e *Engine) flushShard(sh *shard) {
	type ref struct {
		key string
		w   window.Window
		st  *window.State
	}
	var all []ref
	for key, byKey := range sh.states {
		for w, st := range byKey {
			all = append(all, ref{key: key, w: w, st: st})
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].key != all[j].key {
			return all[i].key < all[j].key
		}
		return all[i].w.Start.Before(all[j].w.Start)
	})
	for _, r := range all {
		e.commitWindow(sh, r.key, r.w, r.st)
	}
}

// commitWindow 幂等提交一个窗口：先查引擎恢复的已提交集合，再依赖下游幂等。
func (e *Engine) commitWindow(sh *shard, key string, w window.Window, st *window.State) {
	ref := checkpoint.WindowRef{Key: key, Start: w.Start, End: w.End}

	e.committedMu.Lock()
	_, already := e.committed[ref]
	e.committedMu.Unlock()
	if already {
		// 检查点恢复后已提交的窗口：不重复输出，直接清除重建出的中间态。
		delete(sh.states[key], w)
		if len(sh.states[key]) == 0 {
			delete(sh.states, key)
		}
		return
	}

	committed, err := e.snk.Write(e.ctx, st.Result(key, w))
	// err 只可能发生在关停（ctx 取消）时：状态丢弃且不登记已提交，
	// 避免“未写出却记入检查点”导致恢复后静默丢失。
	if err == nil {
		e.committedMu.Lock()
		e.committed[ref] = struct{}{}
		e.committedMu.Unlock()
		if committed {
			e.windowsEmitted.Add(1)
		}
	}
	delete(sh.states[key], w)
	if len(sh.states[key]) == 0 {
		delete(sh.states, key)
	}
}

// barrierCheckpoint 暂停全部分片，在静态点拍摄快照后恢复。
//
// 静态点保证不变式：凡 deadline <= watermark 的窗口都已在已提交集合中，
// 因此恢复时“旧水位线 + 重放 + 幂等去重”不会丢失或重复任何窗口结果。
func (e *Engine) barrierCheckpoint(ctx context.Context) (*checkpoint.Checkpoint, error) {
	if e.store == nil {
		return nil, nil
	}
	b := &barrier{
		ack:    make(chan struct{}, len(e.shards)),
		resume: make(chan struct{}),
	}
	for _, sh := range e.shards {
		select {
		case sh.ch <- shardMsg{barrier: b}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	for range e.shards {
		select {
		case <-b.ack:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	cp, err := e.saveSnapshot(ctx)
	close(b.resume)
	return cp, err
}

func (e *Engine) saveSnapshot(ctx context.Context) (*checkpoint.Checkpoint, error) {
	e.committedMu.Lock()
	refs := make([]checkpoint.WindowRef, 0, len(e.committed))
	for ref := range e.committed {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Key != refs[j].Key {
			return refs[i].Key < refs[j].Key
		}
		return refs[i].Start.Before(refs[j].Start)
	})
	e.committedMu.Unlock()

	id := e.checkpoints.Add(1)
	cp := &checkpoint.Checkpoint{
		ID:               id,
		Watermark:        e.wm.Current(),
		MaxSeq:           e.maxSeq.Load(),
		CommittedWindows: refs,
		CreatedAt:        time.Now(),
	}
	if err := e.store.Save(ctx, cp); err != nil {
		return nil, err
	}
	return cp, nil
}

// Enqueue 将事件提交到引擎入口；缓冲满时立即返回 false 并记录背压丢弃。
//
// 非阻塞语义保证上游再快也不会导致引擎内缓冲无限增长：
// 入口 in、各分片 ch 均有界，满了之后由调用方承担丢弃后果，且丢弃可计数、可审计。
func (e *Engine) Enqueue(ev event.Event) bool {
	if e.closed.Load() || !e.started.Load() {
		return false
	}
	e.sendMu.RLock()
	defer e.sendMu.RUnlock()
	if e.closed.Load() {
		return false
	}
	select {
	case e.in <- ev:
		return true
	default:
		e.reject(ev, event.ReasonBackpressureFull)
		e.backpressureDropped.Add(1)
		return false
	}
}

// Checkpoint 触发一次显式检查点（与事件处理在路由线程串行）。
func (e *Engine) Checkpoint(ctx context.Context) (*checkpoint.Checkpoint, error) {
	if e.closed.Load() || !e.started.Load() {
		return nil, ErrNotRunning
	}
	req := ckptRequest{reply: make(chan ckptResult, 1)}
	select {
	case e.ckptReq <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.quit:
		return nil, ErrNotRunning
	}
	select {
	case r := <-req.reply:
		return r.cp, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.quit:
		return nil, ErrNotRunning
	}
}

// Watermark 返回引擎当前全局事件时间水位线。
func (e *Engine) Watermark() time.Time { return e.wm.Current() }

// Stats 返回当前计数快照。
func (e *Engine) Stats() Stats {
	return Stats{
		Received:            e.received.Load(),
		Accepted:            e.accepted.Load(),
		DuplicateSkipped:    e.duplicateSkipped.Load(),
		LateDropped:         e.lateDropped.Load(),
		BackpressureDropped: e.backpressureDropped.Load(),
		WindowsEmitted:      e.windowsEmitted.Load(),
		Checkpoints:         e.checkpoints.Load(),
	}
}

// Drops 返回被拒绝事件记录的有界样本副本。
func (e *Engine) Drops() []event.DropRecord {
	e.dropsMu.Lock()
	defer e.dropsMu.Unlock()
	return append([]event.DropRecord(nil), e.drops...)
}

func (e *Engine) reject(ev event.Event, reason event.DropReason) {
	e.dropsMu.Lock()
	if len(e.drops) < maxDropRecords {
		e.drops = append(e.drops, event.DropRecord{Event: ev, Reason: reason})
	}
	e.dropsMu.Unlock()
}

// Close 冲刷在途窗口、做最终检查点并停止引擎。
//
// 调用约定：Close 之后不得再调用 Enqueue；调用方应先停止注入。
func (e *Engine) Close() error {
	e.closeOnce.Do(func() {
		e.closed.Store(true)
		// 等待所有在途 Enqueue 退出，再安全关闭 in。
		e.sendMu.Lock()
		close(e.in)
		close(e.quit)
		e.sendMu.Unlock()
	})
	e.routerWg.Wait()
	e.shardWg.Wait()
	if e.cancel != nil {
		e.cancel()
	}
	return nil
}

// Abort 模拟进程崩溃：立即取消处理、丢弃在途缓冲且不冲刷窗口、不做最终检查点。
//
// 已完成的检查点仍是唯一恢复点；窗口是否已输出以检查点与幂等下游为准。
// Abort 后实例不可复用，恢复需用 New 新建引擎并加载同一 checkpoint.Store。
func (e *Engine) Abort() {
	e.abortOnce.Do(func() {
		e.closed.Store(true)
		if e.cancel != nil {
			e.cancel()
		}
		close(e.quit)
	})
	e.routerWg.Wait()
	e.shardWg.Wait()
}

// shardIndex 以 key 的 FNV 哈希选择分片，同一 key 永远落在同一分片。
func shardIndex(key string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}
