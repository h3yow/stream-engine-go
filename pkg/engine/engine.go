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

// ErrIncompatibleCheckpoint 表示检查点无法安全地用于当前配置恢复：
// 例如旧格式检查点缺少并行度元数据（无法验证扩缩容安全性），
// 或检查点版本高于当前引擎支持的版本。出现该错误时必须显式处理，
// 引擎不会“悄悄接着算”。
var ErrIncompatibleCheckpoint = errors.New("engine: incompatible checkpoint")

// RestoreInfo 描述一次启动时的检查点恢复结果，供验收与排障核对。
type RestoreInfo struct {
	Restored         bool  // 是否从检查点恢复（false 表示全新启动）
	CheckpointID     int64 // 恢复所用的检查点 ID
	FromParallelism  int   // 检查点拍摄时的并行度
	ToParallelism    int   // 当前引擎并行度
	Rescaled         bool  // 是否发生了扩缩容恢复（From != To）
	Partitions       int   // 检查点中记录的数据源分片数
	CommittedWindows int   // 恢复出的已提交窗口数
}

// PartitionStatus 为状态快照中单个数据源分片的进展。
type PartitionStatus struct {
	ID          int
	Status      string    // active / idle / finished
	MaxObserved time.Time // 该分片已观测的最大事件时间
	HasData     bool      // 是否观测到过至少一条事件
	// Watermark 为该分片对全局水位线的贡献（MaxObserved - MaxOutOfOrderness）；
	// 仅当分片活跃且有数据时参与全局最小值计算。
	Watermark time.Time
}

// PendingWindow 为状态快照中一个仍在等待定稿的窗口。
type PendingWindow struct {
	Key      string
	Start    time.Time
	End      time.Time
	Sum      int64     // 当前部分聚合和
	Count    int64     // 当前已计入事件数
	Deadline time.Time // 定稿线 = End + AllowedLateness
}

// StatusSnapshot 是运行期状态的一份一致性快照（屏障点拍摄），
// 字段含义稳定，供排障与验收核对。
type StatusSnapshot struct {
	// Watermark 为快照时刻的全局事件时间水位线。
	Watermark time.Time
	// Parallelism 为当前处理分片数。
	Parallelism int
	// Partitions 为各数据源分片进展（按分片 ID 升序）。
	Partitions []PartitionStatus
	// PendingWindows 为仍在等待定稿的窗口（按 key、窗口起始升序）。
	PendingWindows []PendingWindow
	// CommittedWindows 为已定稿提交的窗口总数（含检查点恢复出的）。
	CommittedWindows int
	// Drops 为被拒事件按原因的精确计数。
	Drops map[event.DropReason]int64
	// Stats 为引擎累计计数。
	Stats Stats
	// Restore 为启动时的检查点恢复情况。
	Restore RestoreInfo
}

// Config 为引擎配置。
type Config struct {
	WindowSize        time.Duration // 滚动窗口长度（事件时间）
	MaxOutOfOrderness time.Duration // 水位线允许的最大乱序跨度
	AllowedLateness   time.Duration // 水位线越过窗口结束后，额外接受迟到事件的时长
	InputBuffer       int           // 入口有界缓冲容量（背压边界）
	Parallelism       int           // 处理分片数；同一 key 始终由同一分片串行处理
	CheckpointEveryN  int           // 每接收 N 条事件做一次周期检查点；0 表示关闭
	// PartitionIdleTimeout：>0 时，超过该时长没有新事件的数据源分片
	// 被自动视为空闲，不再拖住全局水位线；0 表示只能用 MarkPartitionIdle 显式标记。
	PartitionIdleTimeout time.Duration
	// SourcePartitions 声明数据源分片数量（分片 ID 为 0..N-1）。
	// >0 时引擎启动即登记全部分片：某分片首次供数（或被标记空闲/终结）之前，
	// 水位线不会越过当前值，避免分片尚未被发现水位线已越过其数据。
	// 0 表示分片集合未知，按事件到达惰性发现（发现前不约束水位线）。
	SourcePartitions int
}

// Stats 为引擎可观测计数。
type Stats struct {
	Received                 int64
	Accepted                 int64
	DuplicateSkipped         int64
	LateDropped              int64
	BackpressureDropped      int64
	PartitionFinishedDropped int64
	WindowsEmitted           int64
	Checkpoints              int64
}

// shardMsg 为路由到分片的消息：要么是一条事件，要么是检查点屏障。
type shardMsg struct {
	ev      event.Event
	barrier *barrier
	// poke 为 true 时表示“水位线已推进”通知：收到通知的分片检查并输出
	// 本分片内已到定稿线的窗口（空闲分片场景下，本分片可能长期没有新事件，
	// 但其上的窗口不应因此无限等待）。
	poke bool
	// shutdown 为 true 时表示关停哨兵：分片冲刷全部在途窗口后退出。
	// 用哨兵消息而非 close(通道)，是因为其他分片可能并发向本通道发 poke，
	// close 会与并发的 send 竞态（send on closed channel）。
	shutdown bool
}

// barrier 是分片间的暂停/恢复屏障，用于拍摄一致检查点或状态快照。
type barrier struct {
	ack    chan struct{} // 每个分片处理到屏障时发送一次
	resume chan struct{} // 关闭后分片继续运行
	// collect 非空时，各分片在屏障点（此前事件均已处理完）调用它上报状态。
	collect func(sh *shard)
}

type shard struct {
	ch     chan shardMsg
	states map[string]map[window.Window]*window.State
	lastWM time.Time // 本分片最近一次观测到的全局水位线（用于检测推进）
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
	snapReq chan snapRequest
	ctx     context.Context

	restoreInfo RestoreInfo

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

	dropsMu       sync.Mutex
	drops         []event.DropRecord
	dropsByReason map[event.DropReason]int64

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

type snapRequest struct {
	reply chan StatusSnapshot
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
		cfg:           cfg,
		snk:           snk,
		store:         store,
		assigner:      window.NewTumblingAssigner(cfg.WindowSize),
		wm:            watermark.NewWithIdleTimeout(cfg.MaxOutOfOrderness, cfg.PartitionIdleTimeout),
		in:            make(chan event.Event, cfg.InputBuffer),
		quit:          make(chan struct{}),
		committed:     make(map[checkpoint.WindowRef]struct{}),
		ckptReq:       make(chan ckptRequest),
		snapReq:       make(chan snapRequest),
		dropsByReason: make(map[event.DropReason]int64),
	}
}

// Start 启动后台处理 goroutine 并从检查点恢复（若存在）。
//
// 恢复语义：加载最近检查点，恢复水位线（含各数据源分片的 idle/finished 状态）
// 与已提交窗口集合；窗口中间态不持久化，要求数据源从最早事件开始重放，
// 由 Seq 去重、已提交窗口集合与下游幂等写共同保证结果不重不丢。
//
// 扩缩容恢复：检查点中的状态（全局水位线、分片水位线、已提交窗口集合、
// MaxSeq）全部与处理并行度无关，因此允许以不同于拍摄时的并行度恢复，
// 最终结果与原并行度下一致（恢复情况可通过 RestoreInfo 核对）。
// 但旧格式检查点（未记录并行度）无法验证这一点，Start 会返回
// ErrIncompatibleCheckpoint 而不是冒险继续。
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
			if err := e.restore(cp); err != nil {
				return err
			}
		}
	}

	// 数据源分片集合已知时启动即登记，水位线在全部分片首次供数前不推进。
	for p := 0; p < e.cfg.SourcePartitions; p++ {
		e.wm.Track(p)
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

// restore 校验检查点兼容性并恢复状态；不兼容时返回 ErrIncompatibleCheckpoint。
func (e *Engine) restore(cp *checkpoint.Checkpoint) error {
	if cp.Version > checkpoint.CurrentVersion {
		return fmt.Errorf("%w: checkpoint version %d > supported %d",
			ErrIncompatibleCheckpoint, cp.Version, checkpoint.CurrentVersion)
	}
	if cp.Parallelism <= 0 {
		// 旧格式检查点没有并行度元数据：状态是否并行度无关无法验证，
		// 明确拒绝而不是悄悄恢复（可删除检查点全新启动，或用旧版本引擎继续）。
		return fmt.Errorf("%w: checkpoint %d 缺少并行度元数据（旧格式），无法验证扩缩容安全性",
			ErrIncompatibleCheckpoint, cp.ID)
	}
	states := make([]watermark.PartitionState, 0, len(cp.Partitions))
	for _, p := range cp.Partitions {
		states = append(states, watermark.PartitionState{
			ID:          p.ID,
			Status:      watermark.PartitionStatus(p.Status),
			MaxObserved: p.MaxObserved,
			HasData:     p.HasData,
		})
	}
	e.wm.RestorePartitions(cp.Watermark, states)
	for _, ref := range cp.CommittedWindows {
		e.committed[ref] = struct{}{}
	}
	e.maxSeq.Store(cp.MaxSeq)
	e.restoreInfo = RestoreInfo{
		Restored:         true,
		CheckpointID:     cp.ID,
		FromParallelism:  cp.Parallelism,
		ToParallelism:    e.cfg.Parallelism,
		Rescaled:         cp.Parallelism != e.cfg.Parallelism,
		Partitions:       len(cp.Partitions),
		CommittedWindows: len(cp.CommittedWindows),
	}
	return nil
}

// RestoreInfo 返回启动时的检查点恢复情况；未从检查点恢复时 Restored=false。
func (e *Engine) RestoreInfo() RestoreInfo { return e.restoreInfo }

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
		case req := <-e.snapReq:
			req.reply <- e.barrierSnapshot(ctx)
		case ev, ok := <-in:
			if !ok {
				// 入口关闭：向各分片发送关停哨兵，分片冲刷剩余窗口后退出。
				for _, sh := range e.shards {
					select {
					case sh.ch <- shardMsg{shutdown: true}:
					case <-ctx.Done():
						return
					}
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
			if !ok || msg.shutdown {
				// 关停哨兵：定稿冲刷，所有仍打开的窗口无条件输出。
				e.flushShard(sh)
				return
			}
			if msg.barrier != nil {
				// 处理到屏障：此前所有事件均已完成（含窗口触发写出）。
				if msg.barrier.collect != nil {
					msg.barrier.collect(sh)
				}
				msg.barrier.ack <- struct{}{}
				select {
				case <-msg.barrier.resume:
				case <-ctx.Done():
					return
				}
				continue
			}
			if msg.poke {
				// 水位线推进通知：输出本分片内已到定稿线的窗口。
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

	// 已终结分片彻底不再供数：其事件一律明确拒绝，原因与迟到可区分。
	if e.wm.IsFinished(ev.Partition) {
		e.reject(ev, event.ReasonPartitionFinished)
		e.partitionFinishedDropped.Add(1)
		return
	}
	// 空闲分片来数据会自动恢复为活跃；水位线按“活跃分片最小值”推进。
	wm := e.wm.ObservePartition(ev.Partition, ev.Timestamp)
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
	e.broadcastIfAdvanced(sh, wm)
}

// broadcastIfAdvanced 在水位线推进时通知其余分片检查各自的到期窗口。
// 非阻塞发送：对端通道满说明它有在途事件，处理时自会检查定稿线，跳过不丢正确性。
func (e *Engine) broadcastIfAdvanced(sh *shard, wm time.Time) {
	if !wm.After(sh.lastWM) {
		return
	}
	sh.lastWM = wm
	for _, other := range e.shards {
		if other == sh {
			continue
		}
		select {
		case other.ch <- shardMsg{poke: true}:
		default:
		}
	}
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

// flushShard 输出分片内所有窗口（关闭时调用）。
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
	parts := e.wm.Partitions()
	cpParts := make([]checkpoint.PartitionState, 0, len(parts))
	for _, p := range parts {
		cpParts = append(cpParts, checkpoint.PartitionState{
			ID:          p.ID,
			Status:      string(p.Status),
			MaxObserved: p.MaxObserved,
			HasData:     p.HasData,
		})
	}
	cp := &checkpoint.Checkpoint{
		ID:               id,
		Version:          checkpoint.CurrentVersion,
		Parallelism:      e.cfg.Parallelism,
		Watermark:        e.wm.Current(),
		MaxSeq:           e.maxSeq.Load(),
		CommittedWindows: refs,
		Partitions:       cpParts,
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

// Snapshot 在屏障点拍摄一份一致性状态快照（水位线、分片进展、待定稿窗口、
// 拒绝分类计数等），供运行期排障与验收使用；引擎关闭后返回 ErrNotRunning。
func (e *Engine) Snapshot(ctx context.Context) (StatusSnapshot, error) {
	if e.closed.Load() || !e.started.Load() {
		return StatusSnapshot{}, ErrNotRunning
	}
	req := snapRequest{reply: make(chan StatusSnapshot, 1)}
	select {
	case e.snapReq <- req:
	case <-ctx.Done():
		return StatusSnapshot{}, ctx.Err()
	case <-e.quit:
		return StatusSnapshot{}, ErrNotRunning
	}
	select {
	case snap := <-req.reply:
		return snap, nil
	case <-ctx.Done():
		return StatusSnapshot{}, ctx.Err()
	case <-e.quit:
		return StatusSnapshot{}, ErrNotRunning
	}
}

// barrierSnapshot 通过屏障在各分片的静态点收集待定稿窗口，
// 与事件处理在路由线程串行，因此快照内部自洽。
func (e *Engine) barrierSnapshot(ctx context.Context) StatusSnapshot {
	var mu sync.Mutex
	var pending []PendingWindow
	b := &barrier{
		ack:    make(chan struct{}, len(e.shards)),
		resume: make(chan struct{}),
		collect: func(sh *shard) {
			mu.Lock()
			defer mu.Unlock()
			for key, byKey := range sh.states {
				for w, st := range byKey {
					r := st.Result(key, w)
					pending = append(pending, PendingWindow{
						Key:      key,
						Start:    w.Start,
						End:      w.End,
						Sum:      r.Sum,
						Count:    r.Count,
						Deadline: w.End.Add(e.cfg.AllowedLateness),
					})
				}
			}
		},
	}
	for _, sh := range e.shards {
		select {
		case sh.ch <- shardMsg{barrier: b}:
		case <-ctx.Done():
			close(b.resume)
			return StatusSnapshot{}
		}
	}
	for range e.shards {
		select {
		case <-b.ack:
		case <-ctx.Done():
			close(b.resume)
			return StatusSnapshot{}
		}
	}
	close(b.resume)

	sort.Slice(pending, func(i, j int) bool {
		if pending[i].Key != pending[j].Key {
			return pending[i].Key < pending[j].Key
		}
		return pending[i].Start.Before(pending[j].Start)
	})

	parts := e.wm.Partitions()
	partStatuses := make([]PartitionStatus, 0, len(parts))
	for _, p := range parts {
		ps := PartitionStatus{
			ID:          p.ID,
			Status:      string(p.Status),
			MaxObserved: p.MaxObserved,
			HasData:     p.HasData,
		}
		if p.HasData {
			ps.Watermark = p.MaxObserved.Add(-e.cfg.MaxOutOfOrderness)
		}
		partStatuses = append(partStatuses, ps)
	}

	e.committedMu.Lock()
	committed := len(e.committed)
	e.committedMu.Unlock()

	return StatusSnapshot{
		Watermark:        e.wm.Current(),
		Parallelism:      e.cfg.Parallelism,
		Partitions:       partStatuses,
		PendingWindows:   pending,
		CommittedWindows: committed,
		Drops:            e.DropCounts(),
		Stats:            e.Stats(),
		Restore:          e.restoreInfo,
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

// DropCounts 返回按原因分类的被拒事件精确计数（不受明细样本上限影响）。
func (e *Engine) DropCounts() map[event.DropReason]int64 {
	e.dropsMu.Lock()
	defer e.dropsMu.Unlock()
	out := make(map[event.DropReason]int64, len(e.dropsByReason))
	for r, n := range e.dropsByReason {
		out[r] = n
	}
	return out
}

// MarkPartitionIdle 把数据源分片标记为暂时空闲：
// 该分片被排除在水位线推进之外，全局水位线跟随其余活跃分片继续前进；
// 分片再次来数据时自动恢复活跃，其事件按正常迟到规则计入或拒绝。
func (e *Engine) MarkPartitionIdle(p int) { e.wm.MarkIdle(p) }

// MarkPartitionFinished 把数据源分片标记为彻底不再供数：
// 该分片被永久排除在水位线推进之外，之后到达的该分片事件
// 一律以 partition_finished 原因明确拒绝（与迟到拒绝可区分）。
func (e *Engine) MarkPartitionFinished(p int) { e.wm.MarkFinished(p) }

// Drops 返回被拒绝事件记录的有界样本副本。
func (e *Engine) Drops() []event.DropRecord {
	e.dropsMu.Lock()
	defer e.dropsMu.Unlock()
	return append([]event.DropRecord(nil), e.drops...)
}

func (e *Engine) reject(ev event.Event, reason event.DropReason) {
	e.dropsMu.Lock()
	e.dropsByReason[reason]++
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
