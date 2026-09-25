// Package window 提供事件时间滚动窗口分配与窗口聚合状态。
package window

import (
	"sort"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/event"
)

// Window 表示一个 [Start, End) 事件时间区间。
type Window struct {
	Start time.Time
	End   time.Time
}

// Result 是某个 key 在某个窗口的聚合结果。
type Result struct {
	Key    string
	Window Window
	Sum    int64
	Count  int64
	// Seqs 为计入该结果的事件序号集合，用于结果可解释与审计。
	Seqs []int64
}

// Assigner 为滚动事件时间窗口分配器。
type Assigner struct {
	size time.Duration
}

// NewTumblingAssigner 创建长度为 size 的滚动窗口分配器。
func NewTumblingAssigner(size time.Duration) *Assigner {
	return &Assigner{size: size}
}

// Assign 返回时间戳 ts 所属的窗口。
//
// 窗口边界按 Unix 纳秒对 size 取整，与绝对时刻无关，
// 因此同一 ts 无论何时、以何种顺序到达都会得到同一窗口。
func (a *Assigner) Assign(ts time.Time) Window {
	ns := ts.UnixNano()
	sz := a.size.Nanoseconds()
	start := ns - floorMod(ns, sz)
	return Window{
		Start: time.Unix(0, start).UTC(),
		End:   time.Unix(0, start+sz).UTC(),
	}
}

// floorMod 计算数学意义上的向下取余（对负数也成立）。
func floorMod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// Size 返回窗口长度。
func (a *Assigner) Size() time.Duration { return a.size }

// State 保存单个 (key, window) 的聚合内容与已计入事件序号集合（去重）。
type State struct {
	sum   int64
	count int64
	seqs  map[int64]struct{}
}

// NewState 创建空的窗口聚合状态。
func NewState() *State {
	return &State{seqs: make(map[int64]struct{})}
}

// Add 将事件计入窗口；若该序号已计入则返回 false（重复事件）。
//
// 以 Seq 作为事件身份：同一 Seq 无论被投递多少次，只计入一次，
// 保证“重复投递”与“只投递一次”结果一致。
func (s *State) Add(ev event.Event) bool {
	if _, dup := s.seqs[ev.Seq]; dup {
		return false
	}
	s.seqs[ev.Seq] = struct{}{}
	s.sum += ev.Value
	s.count++
	return true
}

// Result 导出当前聚合结果。
func (s *State) Result(key string, w Window) Result {
	return Result{
		Key:    key,
		Window: w,
		Sum:    s.sum,
		Count:  s.count,
		Seqs:   s.Seqs(),
	}
}

// Seqs 返回已计入事件序号的有序副本。
func (s *State) Seqs() []int64 {
	out := make([]int64, 0, len(s.seqs))
	for seq := range s.seqs {
		out = append(out, seq)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
