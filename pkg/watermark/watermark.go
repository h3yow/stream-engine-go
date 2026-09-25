// Package watermark 提供基于事件时间的水位线生成。
package watermark

import (
	"sync"
	"time"
)

// Generator 采用“有界乱序”策略：watermark = 已观测最大事件时间 - maxOutOfOrderness。
type Generator struct {
	mu                sync.Mutex
	maxOutOfOrderness time.Duration
	maxObserved       time.Time
	started           bool
}

// New 创建水位线生成器。
func New(maxOutOfOrderness time.Duration) *Generator {
	return &Generator{maxOutOfOrderness: maxOutOfOrderness}
}

// Observe 观测一条事件时间，返回推进后的当前水位线（单调不减）。
//
// 水位线语义为断言：“事件时间 <= watermark 的事件（在允许迟到范围外）
// 大概率已到齐”。乱序跨度不超过 maxOutOfOrderness 的事件不会被误判为迟到。
func (g *Generator) Observe(ts time.Time) time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.started || ts.After(g.maxObserved) {
		g.maxObserved = ts
		g.started = true
	}
	return g.lockedCurrent()
}

// Current 返回当前水位线。
func (g *Generator) Current() time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.lockedCurrent()
}

// Restore 从检查点恢复水位线：等价于已观测到 watermark+maxOutOfOrderness。
// 恢复后水位线从 cp 记录值继续单调推进，不会回退。
func (g *Generator) Restore(wm time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if wm.IsZero() {
		return
	}
	obs := wm.Add(g.maxOutOfOrderness)
	if !g.started || obs.After(g.maxObserved) {
		g.maxObserved = obs
		g.started = true
	}
}

// lockedCurrent 必须在持有 mu 时调用。
func (g *Generator) lockedCurrent() time.Time {
	if !g.started {
		// 尚未观测任何事件时，水位线为零值，表示“没有任何时间点已被声明到齐”。
		return time.Time{}
	}
	return g.maxObserved.Add(-g.maxOutOfOrderness)
}
