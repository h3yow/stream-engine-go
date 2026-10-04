// Package watermark 提供基于事件时间的分片感知水位线生成。
package watermark

import (
	"sort"
	"sync"
	"time"
)

// PartitionStatus 描述一个数据源分片在水位线跟踪中的状态。
type PartitionStatus string

const (
	// StatusActive 表示分片正常供数，参与水位线最小值计算。
	StatusActive PartitionStatus = "active"
	// StatusIdle 表示分片暂时无数据，被排除在水位线计算之外；
	// 分片再次来数据时自动恢复为 active，其事件按正常迟到规则处理。
	StatusIdle PartitionStatus = "idle"
	// StatusFinished 表示分片已彻底不再供数，被永久排除；
	// 与 idle 不同，finished 不会因新事件自动恢复，
	// 其后续事件由引擎以 partition_finished 原因明确拒绝。
	StatusFinished PartitionStatus = "finished"
)

// PartitionState 是单个分片的水位线跟踪状态快照（也用于检查点持久化）。
type PartitionState struct {
	ID          int
	Status      PartitionStatus
	MaxObserved time.Time // 该分片已观测的最大事件时间
	HasData     bool      // 是否观测到过至少一条事件
}

// partition 为分片的内部可变状态。
type partition struct {
	maxObserved time.Time
	hasData     bool
	status      PartitionStatus
	lastEventAt time.Time // 最近一次事件到达的处理时间（用于空闲超时判定）
}

// Generator 采用“有界乱序 + 分片最小值”策略：
//
//	watermark = min over 活跃分片 ( 分片最大事件时间 - maxOutOfOrderness )
//
// 单分片时退化为经典的 watermark = maxObserved - maxOutOfOrderness。
// 空闲（idle）与终结（finished）分片不参与最小值计算，
// 因此个别分片长时间无数据不会拖住全局水位线。
// 水位线单调不减：内部 floor 记录历史最高水位线，绝不回退。
type Generator struct {
	mu                sync.Mutex
	maxOutOfOrderness time.Duration
	idleTimeout       time.Duration // >0 时按处理时间自动把沉默分片标记为 idle
	parts             map[int]*partition
	floor             time.Time // 历史最高水位线（含恢复值），保证单调
	now               func() time.Time
}

// New 创建水位线生成器（不启用空闲超时，空闲只能显式标记）。
func New(maxOutOfOrderness time.Duration) *Generator {
	return NewWithIdleTimeout(maxOutOfOrderness, 0)
}

// NewWithIdleTimeout 创建水位线生成器；idleTimeout>0 时，
// 超过该时长没有新事件的活跃分片在下一次观测时被自动标记为 idle。
func NewWithIdleTimeout(maxOutOfOrderness, idleTimeout time.Duration) *Generator {
	return &Generator{
		maxOutOfOrderness: maxOutOfOrderness,
		idleTimeout:       idleTimeout,
		parts:             make(map[int]*partition),
		now:               time.Now,
	}
}

// Observe 观测一条来自分片 0 的事件时间（单分片数据源的便捷形式）。
func (g *Generator) Observe(ts time.Time) time.Time { return g.ObservePartition(0, ts) }

// ObservePartition 观测一条来自分片 p 的事件时间，返回推进后的当前水位线。
//
// 空闲分片来数据时自动恢复为 active；已终结分片的观测被忽略
// （其事件是否拒绝由引擎层判定，这里只保证水位线不再受它影响）。
func (g *Generator) ObservePartition(p int, ts time.Time) time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sweepIdleLocked()
	pt := g.getLocked(p)
	if pt.status == StatusFinished {
		return g.currentLocked()
	}
	if pt.status == StatusIdle {
		pt.status = StatusActive // 空闲分片恢复供数
	}
	if !pt.hasData || ts.After(pt.maxObserved) {
		pt.maxObserved = ts
		pt.hasData = true
	}
	pt.lastEventAt = g.now()
	return g.currentLocked()
}

// MarkIdle 把分片标记为暂时空闲：从水位线计算中排除，来数据可自动恢复。
// 对不存在的分片也可标记（登记为无数据的空闲分片）；对已终结分片无效。
func (g *Generator) MarkIdle(p int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	pt := g.getLocked(p)
	if pt.status != StatusFinished {
		pt.status = StatusIdle
	}
}

// MarkFinished 把分片标记为彻底不再供数：永久从水位线计算中排除，
// 之后该分片的观测一律被忽略（引擎层会以 partition_finished 拒绝其事件）。
func (g *Generator) MarkFinished(p int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.getLocked(p).status = StatusFinished
}

// Track 显式登记一个预期存在的分片（数据源分片集合已知时，启动即登记）。
//
// 已登记但还没有任何数据的活跃分片会把水位线压在当前值：
// 水位线语义是“min over 活跃分片”，一个从未供数的分片其水位线贡献
// 视为负无穷，因此在它首次供数（或被标记 idle/finished）之前，
// 全局水位线不能推进。这避免了“分片尚未被发现，水位线已越过其数据”。
func (g *Generator) Track(p int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	pt := g.getLocked(p)
	if !pt.hasData && pt.lastEventAt.IsZero() {
		pt.lastEventAt = g.now() // 让空闲超时对从未供数的分片同样生效
	}
}

// IsFinished 报告分片是否已终结。
func (g *Generator) IsFinished(p int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	pt, ok := g.parts[p]
	return ok && pt.status == StatusFinished
}

// Current 返回当前水位线（单调不减）。
//
// 水位线语义为断言：“事件时间 <= watermark 的事件（在允许迟到范围外）
// 大概率已到齐”。没有任何活跃分片供数时，水位线保持在历史最高值。
func (g *Generator) Current() time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.currentLocked()
}

// Partitions 返回各分片跟踪状态的有序快照（按分片 ID 升序）。
func (g *Generator) Partitions() []PartitionState {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]PartitionState, 0, len(g.parts))
	for id, pt := range g.parts {
		out = append(out, PartitionState{
			ID:          id,
			Status:      pt.status,
			MaxObserved: pt.maxObserved,
			HasData:     pt.hasData,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Restore 从检查点恢复水位线（单分片兼容形式）：
// 等价于分片 0 已观测到 watermark+maxOutOfOrderness，且 floor 提升到 watermark。
func (g *Generator) Restore(wm time.Time) {
	g.RestorePartitions(wm, nil)
}

// RestorePartitions 从检查点恢复：floor 提升到 wm（保证不回退），
// 并按检查点记录重建各分片状态（含 idle/finished 标记）。
func (g *Generator) RestorePartitions(wm time.Time, states []PartitionState) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !wm.IsZero() && wm.After(g.floor) {
		g.floor = wm
	}
	for _, s := range states {
		pt := g.getLocked(s.ID)
		pt.status = s.Status
		if pt.status == "" {
			pt.status = StatusActive
		}
		pt.maxObserved = s.MaxObserved
		pt.hasData = s.HasData
		pt.lastEventAt = g.now()
	}
	// 兼容旧检查点（无分片明细）：登记分片 0 已观测到 wm+ooo，
	// 使后续推进语义与单分片生成器一致。
	if len(states) == 0 && !wm.IsZero() {
		pt := g.getLocked(0)
		obs := wm.Add(g.maxOutOfOrderness)
		if !pt.hasData || obs.After(pt.maxObserved) {
			pt.maxObserved = obs
			pt.hasData = true
		}
	}
}

// getLocked 取分片状态，不存在则创建为 active。必须持有 mu。
func (g *Generator) getLocked(p int) *partition {
	pt, ok := g.parts[p]
	if !ok {
		pt = &partition{status: StatusActive}
		g.parts[p] = pt
	}
	return pt
}

// sweepIdleLocked 按处理时间把沉默超过 idleTimeout 的活跃分片标记为 idle。
// 必须持有 mu。
func (g *Generator) sweepIdleLocked() {
	if g.idleTimeout <= 0 {
		return
	}
	now := g.now()
	for _, pt := range g.parts {
		if pt.status == StatusActive && pt.hasData &&
			now.Sub(pt.lastEventAt) > g.idleTimeout {
			pt.status = StatusIdle
		}
	}
}

// currentLocked 计算当前水位线并抬升 floor。必须持有 mu。
func (g *Generator) currentLocked() time.Time {
	wm := g.floor
	first := true
	for _, pt := range g.parts {
		if pt.status != StatusActive {
			continue
		}
		if !pt.hasData {
			// 已登记但从未供数的活跃分片：贡献视为负无穷，
			// 水位线不能越过当前 floor。
			return g.floor
		}
		cand := pt.maxObserved.Add(-g.maxOutOfOrderness)
		if first || cand.Before(wm) {
			wm = cand
			first = false
		}
	}
	// 没有任何活跃供数分片时保持 floor；有候选值时取 max(floor, min(候选))。
	if first {
		return g.floor
	}
	if wm.Before(g.floor) {
		wm = g.floor
	}
	if wm.After(g.floor) {
		g.floor = wm
	}
	return wm
}
