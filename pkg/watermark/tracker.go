// Package watermark 提供基于事件时间的水位线生成。
package watermark

import (
	"sort"
	"sync"
	"time"
)

// PartitionStatus 描述数据源分片的供数状态。
type PartitionStatus string

const (
	// StatusActive 表示分片在正常供数，参与全局水位线计算。
	StatusActive PartitionStatus = "active"
	// StatusIdle 表示分片暂时无数据（超过空闲阈值未观测到事件），
	// 暂时从全局水位线计算中排除；恢复供数后自动回到 active。
	StatusIdle PartitionStatus = "idle"
	// StatusFinished 表示分片已被显式宣告彻底不再供数，
	// 永久从全局水位线计算中排除，且不再接受该分片的事件。
	StatusFinished PartitionStatus = "finished"
)

// PartitionInfo 是单个数据源分片的水位线进展快照。
type PartitionInfo struct {
	ID     string          // 分片标识（空串为未分片数据源的默认分片）
	Status PartitionStatus // 供数状态：active / idle / finished
	// MaxObserved 为该分片已观测的最大事件时间；未观测到事件时为零值。
	MaxObserved time.Time
	// Watermark 为该分片贡献的水位线（MaxObserved - maxOutOfOrderness）。
	Watermark time.Time
	// LastEventAt 为该分片最近一次观测到事件的处理时间（空闲判定依据）。
	LastEventAt time.Time
}

// partState 为分片的内部可变状态。
type partState struct {
	maxObserved time.Time
	started     bool
	lastEventAt time.Time
	finished    bool
}

// Tracker 按数据源分片分别跟踪事件时间进展，合成全局水位线：
//
//	全局水位线 = min(各 active 分片水位线)，且单调不减。
//
// 与单生成器 Generator 的区别在于多分片语义：
//   - 某分片超过 idleTimeout 未供数即视为 idle，被排除出 min 计算，
//     全局水位线随其余正常分片继续推进；该分片恢复供数后自动重新参与；
//   - 被 Finish 的分片永久排除，其后续事件由 Observe 报告 ok=false 拒绝；
//   - 所有分片都 idle/finished 时，全局水位线保持在最近值不再推进。
type Tracker struct {
	mu                sync.Mutex
	maxOutOfOrderness time.Duration
	idleTimeout       time.Duration
	now               func() time.Time
	parts             map[string]*partState
	wm                time.Time // 当前全局水位线（单调不减）
	hasWM             bool
}

// NewTracker 创建分片水位线跟踪器；idleTimeout<=0 表示不做空闲检测。
func NewTracker(maxOutOfOrderness, idleTimeout time.Duration) *Tracker {
	return NewTrackerWithClock(maxOutOfOrderness, idleTimeout, time.Now)
}

// NewTrackerWithClock 同 NewTracker，但可注入处理时间时钟（用于确定性测试）。
func NewTrackerWithClock(maxOutOfOrderness, idleTimeout time.Duration, now func() time.Time) *Tracker {
	return &Tracker{
		maxOutOfOrderness: maxOutOfOrderness,
		idleTimeout:       idleTimeout,
		now:               now,
		parts:             make(map[string]*partState),
	}
}

// Observe 观测分片 partition 的一条事件时间，返回推进后的全局水位线。
//
// 若该分片已被 Finish 宣告终止，返回 ok=false：调用方应以
// partition_finished 原因拒绝该事件，分片状态与水位线均不受影响。
func (t *Tracker) Observe(partition string, ts time.Time) (wm time.Time, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.parts[partition]
	if p == nil {
		p = &partState{}
		t.parts[partition] = p
	}
	if p.finished {
		return t.wmOrZero(), false
	}
	if !p.started || ts.After(p.maxObserved) {
		p.maxObserved = ts
		p.started = true
	}
	p.lastEventAt = t.now()
	t.recomputeLocked()
	return t.wmOrZero(), true
}

// Finish 显式宣告分片彻底不再供数。
//
// 与 idle（暂时无数据、可自动恢复）不同，finished 不可逆：
// 分片被永久排除出水位线计算，其后的 Observe 一律返回 ok=false。
// 对从未观测过的分片调用 Finish 同样生效（登记为 finished）。
func (t *Tracker) Finish(partition string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.parts[partition]
	if p == nil {
		p = &partState{}
		t.parts[partition] = p
	}
	p.finished = true
	t.recomputeLocked()
}

// Current 返回当前全局水位线；调用时先按处理时间重估各分片空闲状态。
func (t *Tracker) Current() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.recomputeLocked()
	return t.wmOrZero()
}

// Partitions 返回各分片状态快照，按分片 ID 排序，字段含义稳定可供排障。
func (t *Tracker) Partitions() []PartitionInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.recomputeLocked()
	out := make([]PartitionInfo, 0, len(t.parts))
	for id, p := range t.parts {
		info := PartitionInfo{ID: id, Status: t.statusLocked(p), LastEventAt: p.lastEventAt}
		if p.started {
			info.MaxObserved = p.maxObserved
			info.Watermark = p.maxObserved.Add(-t.maxOutOfOrderness)
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Restore 从检查点恢复：parts 为各分片已观测的最大事件时间，
// finished 为已被宣告彻底不再供数的分片清单，
// wm 为快照时刻的全局水位线（兜底，保证恢复后水位线不回退）。
//
// 恢复的分片一律以 active 状态重新开始空闲计时（idle 是处理时间上的
// 瞬时判定，恢复后静默超过 idleTimeout 会再次自动转入 idle）；
// 而 finished 是持久宣告，必须原样恢复：这些分片（包括从未供数就被
// 终止、因此不在 parts 中的分片）恢复后依旧永久排除，Observe 返回 ok=false。
func (t *Tracker) Restore(parts map[string]time.Time, finished []string, wm time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	for id, maxObs := range parts {
		p := t.parts[id]
		if p == nil {
			p = &partState{}
			t.parts[id] = p
		}
		if !p.started || maxObs.After(p.maxObserved) {
			p.maxObserved = maxObs
			p.started = true
		}
		p.lastEventAt = now
	}
	for _, id := range finished {
		p := t.parts[id]
		if p == nil {
			p = &partState{}
			t.parts[id] = p
		}
		p.finished = true
	}
	if !wm.IsZero() && (!t.hasWM || wm.After(t.wm)) {
		t.wm = wm
		t.hasWM = true
	}
	t.recomputeLocked()
}

// Finished 返回当前已宣告终止的分片 ID（排序、非 nil），供检查点持久化。
func (t *Tracker) Finished() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.parts))
	for id, p := range t.parts {
		if p.finished {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// recomputeLocked 重估空闲状态并重算全局水位线（单调不减）。必须在持有 mu 时调用。
func (t *Tracker) recomputeLocked() {
	now := t.now()
	var best time.Time
	hasActive := false
	for _, p := range t.parts {
		if p.finished || !p.started {
			continue
		}
		if t.idleTimeout > 0 && now.Sub(p.lastEventAt) > t.idleTimeout {
			continue // idle：暂时排除
		}
		cand := p.maxObserved.Add(-t.maxOutOfOrderness)
		if !hasActive || cand.Before(best) {
			best = cand
			hasActive = true
		}
	}
	// 无 active 分片时水位线保持最近值；有则取 min 且不回退。
	if hasActive && (!t.hasWM || best.After(t.wm)) {
		t.wm = best
		t.hasWM = true
	}
}

// statusLocked 计算分片当前状态。必须在持有 mu 时调用。
func (t *Tracker) statusLocked(p *partState) PartitionStatus {
	if p.finished {
		return StatusFinished
	}
	if t.idleTimeout > 0 && p.started && t.now().Sub(p.lastEventAt) > t.idleTimeout {
		return StatusIdle
	}
	return StatusActive
}

// wmOrZero 返回当前全局水位线；尚未形成任何水位线时返回零值。
func (t *Tracker) wmOrZero() time.Time {
	if !t.hasWM {
		return time.Time{}
	}
	return t.wm
}
