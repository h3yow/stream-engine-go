// Package engine 把数据源、事件时间窗口、水位线、检查点与下游编排为完整引擎。
package engine

import (
	"context"
	"errors"
	"fmt"
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

// ErrIncompatibleCheckpoint 表示检查点无法安全恢复：
// 快照的窗口语义参数（WindowSize/MaxOutOfOrderness/AllowedLateness）
// 与引擎当前配置不一致，继续恢复会静默算错，必须显式拒绝。
// 注意：Parallelism 不同不属于不兼容——已提交窗口集合与水位线
// 都是并行度无关的状态，允许换并行度（扩缩容）恢复。
var ErrIncompatibleCheckpoint = errors.New("engine: incompatible checkpoint")

// checkCompatible 校验检查点与引擎配置的兼容性，不兼容时返回带明细的错误。
func checkCompatible(cp *checkpoint.Checkpoint, cfg Config) error {
	if cp.WindowSize == cfg.WindowSize &&
		cp.MaxOutOfOrderness == cfg.MaxOutOfOrderness &&
		cp.AllowedLateness == cfg.AllowedLateness {
		return nil
	}
	return fmt.Errorf("%w: 检查点语义参数(windowSize=%v maxOutOfOrderness=%v allowedLateness=%v) "+
		"与引擎配置(windowSize=%v maxOutOfOrderness=%v allowedLateness=%v)不一致",
		ErrIncompatibleCheckpoint,
		cp.WindowSize, cp.MaxOutOfOrderness, cp.AllowedLateness,
		cfg.WindowSize, cfg.MaxOutOfOrderness, cfg.AllowedLateness)
}

// Config 为引擎配置。
type Config struct {
	WindowSize        time.Duration // 滚动窗口长度（事件时间）
	MaxOutOfOrderness time.Duration // 水位线允许的最大乱序跨度
	AllowedLateness   time.Duration // 水位线越过窗口结束后，额外接受迟到事件的时长
	InputBuffer       int           // 入口有界缓冲容量（背压边界）
	Parallelism       int           // 处理分片数；同一 key 始终由同一分片串行处理
	CheckpointEveryN  int           // 每接收 N 条事件做一次周期检查点；0 表示关闭
	// IdleTimeout 为数据源分片空闲阈值（处理时间）：某分片超过该时长未供数
	// 即视为 idle，暂时排除出全局水位线计算；<=0 表示不做空闲检测。
	IdleTimeout time.Duration
}

// Stats 为引擎可观测计数。
type Stats struct {
	Received            int64
	Accepted            int64
	DuplicateSkipped    int64
	LateDropped         int64
	BackpressureDropped int64
	// PartitionFinishedDropped 为来自已宣告终止分片而被拒绝的事件数。
	PartitionFinishedDropped int64
	WindowsEmitted           int64
	Checkpoints              int64
}

// shardMsg 为路由到分片的消息：一条事件、一次定稿冲刷通知，或检查点屏障。
type shardMsg struct {
	ev      event.Event
	emit    bool // 水位线已推进：通知分片冲刷到期的窗口
	barrier *barrier
}

// barrier 是分片间的对齐标记：分片处理到它时，之前路由来的全部事件都已完成。
// flush=true 时分片先按 flushWM 精确冲刷到期窗口再 ack。
type barrier struct {
	ack     chan struct{} // 每个分片处理到标记时发送一次（缓冲=分片数，不阻塞）
	flush   bool          // 处理到标记时先按 flushWM 冲刷到期窗口
	flushWM time.Time
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
	wm       *watermark.Tracker

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

	ckptReq  chan ckptRequest
	snapReq  chan snapRequest
	wmNotify chan struct{} // 分片→路由的水位线推进通知（容量1，非阻塞投递）
	ctx      context.Context

	// 原子计数器
	received                 atomic.Int64
	accepted                 atomic.Int64
	duplicateSkipped         atomic.Int64
	lateDropped              atomic.Int64
	backpressureDropped      atomic.Int64
	partitionFinishedDropped atomic.Int64
	windowsEmitted           atomic.Int64
	checkpoints              atomic.Int64
	maxSeq                   atomic.Int64

	dropsMu sync.Mutex
	drops   []event.DropRecord

	committedMu sync.Mutex
	committed   map[checkpoint.WindowRef]struct{}

	openMu   sync.Mutex
	openRefs map[checkpoint.WindowRef]struct{} // 仍在等待定稿的窗口（快照用）
}

type ckptRequest struct {
	reply chan ckptResult
}

type ckptResult struct {
	cp  *checkpoint.Checkpoint
	err error
}

// snapRequest 为一次一致快照请求：经路由线程以屏障协议在分片暂停点拍摄。
type snapRequest struct {
	reply chan Snapshot
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
		wm:        watermark.NewTracker(cfg.MaxOutOfOrderness, cfg.IdleTimeout),
		in:        make(chan event.Event, cfg.InputBuffer),
		quit:      make(chan struct{}),
		committed: make(map[checkpoint.WindowRef]struct{}),
		openRefs:  make(map[checkpoint.WindowRef]struct{}),
		ckptReq:   make(chan ckptRequest),
		snapReq:   make(chan snapRequest),
		wmNotify:  make(chan struct{}, 1),
	}
}

// Start 启动后台处理 goroutine 并从检查点恢复（若存在）。
//
// 恢复语义：加载最近检查点，恢复水位线、各数据源分片供数状态
// （含终止标记与终止位点）与已提交窗口集合；
// 窗口中间态不持久化，要求数据源从最早事件开始重放，
// 由 Seq 去重、已提交窗口集合、分片终止位点与下游幂等写共同保证结果不重不丢：
//   - 已终止分片恢复后保持终止，其终止位点之后的事件仍以 partition_finished 拒绝；
//   - 终止位点之内的重放事件重新接受，用于完整重建检查点时尚未定稿的窗口。
func (e *Engine) Start(ctx context.Context) error {
	if !e.started.CompareAndSwap(false, true) {
		return errors.New("engine: already started")
	}
	e.ctx, e.cancel = context.WithCancel(ctx)
	if e.store != nil {
		cp, err := e.store.LoadLatest(ctx)
		if err != nil {
			e.cancel()
			return err
		}
		if cp != nil {
			if err := checkCompatible(cp, e.cfg); err != nil {
				e.cancel()
				return err
			}
			e.wm.Restore(partitionSnapshots(cp.Partitions), cp.Watermark)
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
//
// 水位线广播：分片处理事件使全局水位线推进后，通过 wmNotify 通知路由线程；
// 路由线程向所有分片广播冲刷通知。这样即使某数据源分片转入空闲、不再产生
// 事件，其 key 所在的处理分片也能随全局水位线推进及时定稿窗口。
// 广播由通知驱动（而非路由侧读取），避免“路由读水位线早于分片处理”的竞态。
func (e *Engine) runRouter(ctx context.Context) {
	defer e.routerWg.Done()
	var routed int64
	var lastEmitWM time.Time
	in := e.in
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.wmNotify:
			if wm := e.wm.Current(); wm.After(lastEmitWM) {
				lastEmitWM = wm
				for _, sh := range e.shards {
					select {
					case sh.ch <- shardMsg{emit: true}:
					case <-ctx.Done():
						return
					}
				}
			}
		case req := <-e.ckptReq:
			cp, err := e.barrierCheckpoint(ctx)
			req.reply <- ckptResult{cp: cp, err: err}
		case req := <-e.snapReq:
			req.reply <- e.barrierSnapshot(ctx)
		case ev, ok := <-in:
			if !ok {
				// 入口关闭：关闭各分片通道，分片会冲刷剩余窗口后退出。
				for _, sh := range e.shards {
					close(sh.ch)
				}
				e.shardWg.Wait()
				// 冲刷完成后做最后一次检查点，固化全部已提交窗口。
				if e.store != nil {
					_, _ = e.saveSnapshot(ctx, e.wm.Current())
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
				// 处理到对齐标记：此前路由来的所有事件均已完成。
				// flush 标记按路由线程给定的同一水位线冲刷到期窗口——
				// 各分片不自行读取水位线，避免“读到的水位线新旧不一”
				// 导致同一割切下有的分片定稿、有的没定稿。
				if msg.barrier.flush {
					e.emitDue(sh, msg.barrier.flushWM)
				}
				select {
				case msg.barrier.ack <- struct{}{}:
				case <-ctx.Done():
					return
				}
				continue
			}
			if msg.emit {
				// 水位线推进通知：冲刷本分片内到期的窗口（无到期则为空操作）。
				e.emitDue(sh, e.wm.Current())
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

	wm, ok := e.wm.Observe(ev.Partition, ev.Timestamp, ev.Seq)
	if !ok {
		// 分片已被显式宣告彻底不再供数：明确拒绝，原因与迟到/背压可区分。
		e.reject(ev, event.ReasonPartitionFinished)
		e.partitionFinishedDropped.Add(1)
		return
	}
	// 水位线可能已推进：通知路由线程向所有分片广播冲刷（非阻塞，容量1去抖）。
	select {
	case e.wmNotify <- struct{}{}:
	default:
	}
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
		e.openMu.Lock()
		e.openRefs[checkpoint.WindowRef{Key: ev.Key, Start: w.Start, End: w.End}] = struct{}{}
		e.openMu.Unlock()
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
		e.openMu.Lock()
		delete(e.openRefs, ref)
		e.openMu.Unlock()
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
	e.openMu.Lock()
	delete(e.openRefs, ref)
	e.openMu.Unlock()
}

// FinishPartition 显式宣告某个数据源分片彻底不再供数。
//
// 与空闲（idle，暂时无数据、恢复供数后自动回归）不同，该宣告不可逆：
// 分片被永久排除出全局水位线计算，其后续事件一律以
// partition_finished 原因拒绝（计数 Stats.PartitionFinishedDropped）。
func (e *Engine) FinishPartition(id string) {
	e.wm.Finish(id)
}

// barrierCheckpoint 在一致的割切点拍摄检查点。
//
// 割切由两阶段对齐协议产生（见 barrierCut）：静态点保证不变式——
// 凡 deadline <= watermark 的窗口都已在已提交集合中，
// 因此恢复时“旧水位线 + 重放 + 幂等去重”不会丢失或重复任何窗口结果。
func (e *Engine) barrierCheckpoint(ctx context.Context) (*checkpoint.Checkpoint, error) {
	if e.store == nil {
		return nil, nil
	}
	wm, ok := e.barrierCut(ctx)
	if !ok {
		return nil, ctx.Err()
	}
	return e.saveSnapshot(ctx, wm)
}

// barrierSnapshot 用与检查点相同的两阶段对齐协议拍摄一致的状态快照：
// 水位线、分片状态、待定/已定稿窗口清单与各项计数都对应同一处理割切，
// 彼此之间不会自相矛盾（例如不会出现“计数已加上、对应窗口却还没出现”，
// 或“窗口定稿线不晚于水位线却仍在待定清单”）。
func (e *Engine) barrierSnapshot(ctx context.Context) Snapshot {
	wm, ok := e.barrierCut(ctx)
	if !ok {
		return e.collectSnapshot()
	}
	return e.collectSnapshotAt(wm)
}

// barrierCut 产生一个全引擎一致的处理割切，返回割切水位线。
//
// 两阶段对齐（路由线程串行执行，期间没有新事件被路由）：
//  1. 对齐标记：各分片处理完标记前的全部事件后 ack——
//     此后没有任何分片还在处理事件，水位线稳定，读取即为割切水位线；
//  2. 冲刷标记：各分片按同一切口水位线精确补齐到期窗口——
//     若让各分片自行读取水位线，先读的分片用旧值、后读的用新值，
//     同一割切下窗口定稿口径不一（快照/检查点自相矛盾的根源）。
//
// 协议结束后分片立即恢复运行；调用方随后读取的状态仍是该割切的静态视图
// （路由线程未返回主循环前不会有新事件进入分片）。
func (e *Engine) barrierCut(ctx context.Context) (time.Time, bool) {
	mark := &barrier{ack: make(chan struct{}, len(e.shards))}
	if !e.broadcast(ctx, mark) {
		return time.Time{}, false
	}
	wm := e.wm.Current()
	flush := &barrier{ack: make(chan struct{}, len(e.shards)), flush: true, flushWM: wm}
	if !e.broadcast(ctx, flush) {
		return time.Time{}, false
	}
	return wm, true
}

// broadcast 向全部处理分片发送对齐标记并等待所有 ack。
func (e *Engine) broadcast(ctx context.Context, b *barrier) bool {
	for _, sh := range e.shards {
		select {
		case sh.ch <- shardMsg{barrier: b}:
		case <-ctx.Done():
			return false
		}
	}
	for range e.shards {
		select {
		case <-b.ack:
		case <-ctx.Done():
			return false
		}
	}
	return true
}

func (e *Engine) saveSnapshot(ctx context.Context, wm time.Time) (*checkpoint.Checkpoint, error) {
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
		ID:                id,
		Watermark:         wm,
		MaxSeq:            e.maxSeq.Load(),
		CommittedWindows:  refs,
		Parallelism:       e.cfg.Parallelism,
		WindowSize:        e.cfg.WindowSize,
		MaxOutOfOrderness: e.cfg.MaxOutOfOrderness,
		AllowedLateness:   e.cfg.AllowedLateness,
		Partitions:        e.partitionProgress(),
		CreatedAt:         time.Now(),
	}
	if err := e.store.Save(ctx, cp); err != nil {
		return nil, err
	}
	return cp, nil
}

// partitionProgress 导出各数据源分片的供数状态（供检查点持久化）。
//
// 已终止分片必须一并导出——包括一条数据都没出过就被停掉的分片
// （MaxObserved 为零值）：漏掉它们会导致恢复后“复活”为正常供数。
func (e *Engine) partitionProgress() map[string]checkpoint.PartitionState {
	infos := e.wm.Partitions()
	out := make(map[string]checkpoint.PartitionState, len(infos))
	for _, info := range infos {
		out[info.ID] = checkpoint.PartitionState{
			MaxObserved:  info.MaxObserved,
			Finished:     info.Status == watermark.StatusFinished,
			FinishSeq:    info.FinishSeq,
			HasFinishSeq: info.HasFinishSeq,
		}
	}
	return out
}

// partitionSnapshots 把检查点中的分片状态转换为水位线跟踪器的恢复输入。
func partitionSnapshots(parts map[string]checkpoint.PartitionState) map[string]watermark.PartitionSnapshot {
	if parts == nil {
		return nil
	}
	out := make(map[string]watermark.PartitionSnapshot, len(parts))
	for id, st := range parts {
		out[id] = watermark.PartitionSnapshot{
			MaxObserved:  st.MaxObserved,
			Finished:     st.Finished,
			FinishSeq:    st.FinishSeq,
			HasFinishSeq: st.HasFinishSeq,
		}
	}
	return out
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
		Received:                 e.received.Load(),
		Accepted:                 e.accepted.Load(),
		DuplicateSkipped:         e.duplicateSkipped.Load(),
		LateDropped:              e.lateDropped.Load(),
		BackpressureDropped:      e.backpressureDropped.Load(),
		PartitionFinishedDropped: e.partitionFinishedDropped.Load(),
		WindowsEmitted:           e.windowsEmitted.Load(),
		Checkpoints:              e.checkpoints.Load(),
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
