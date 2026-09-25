package stream

import "sort"

// WindowManager 负责窗口分配、事件计入、迟到判定与窗口触发。
// 判定规则（watermark 为观察到本事件之前的当前水位线）：
//   - eventTime > watermark：正常事件；
//   - watermark - allowedLateness < eventTime <= watermark：允许范围内的迟到事件，正常计入；
//   - eventTime <= watermark - allowedLateness：超出允许范围，拒绝。
//
// 窗口在水位线越过 window.End + allowedLateness 后触发并关闭。
// 同一事件 ID 只计入一次（幂等去重），保证重复投递与只投递一次结果一致。
type WindowManager struct {
	windowSize      int64
	allowedLateness int64
	windows         map[Window]map[string]*windowState
}

type windowState struct {
	sum   int64
	count int64
	seen  map[string]struct{} // 已计入的事件 ID，用于幂等去重
}

// NewWindowManager 创建窗口管理器。
func NewWindowManager(windowSize, allowedLateness int64) *WindowManager {
	return &WindowManager{
		windowSize:      windowSize,
		allowedLateness: allowedLateness,
		windows:         make(map[Window]map[string]*windowState),
	}
}

// Assign 计算事件所属窗口（滚动窗口，[start, start+size)）。
func (m *WindowManager) Assign(eventTime int64) Window {
	start := eventTime - mod(eventTime, m.windowSize)
	return Window{Start: start, End: start + m.windowSize}
}

// mod 是数学取模（对负数事件时间也正确）。
func mod(a, b int64) int64 {
	r := a % b
	if r < 0 {
		r += b
	}
	return r
}

// Add 将事件计入窗口。返回是否被接受；若拒绝，reason 给出可区分原因。
func (m *WindowManager) Add(ev Event, watermark int64) (accepted bool, reason RejectReason) {
	if ev.EventTime <= watermark-m.allowedLateness {
		return false, RejectLateBeyondAllowed
	}
	w := m.Assign(ev.EventTime)
	keys, ok := m.windows[w]
	if !ok {
		keys = make(map[string]*windowState)
		m.windows[w] = keys
	}
	st, ok := keys[ev.Key]
	if !ok {
		st = &windowState{seen: make(map[string]struct{})}
		keys[ev.Key] = st
	}
	if _, dup := st.seen[ev.ID]; dup {
		return false, RejectDuplicate
	}
	st.seen[ev.ID] = struct{}{}
	st.sum += ev.Value
	st.count++
	return true, ""
}

// Fireable 返回水位线下可触发（可关闭）的窗口列表，按窗口起始时间排序保证确定性。
func (m *WindowManager) Fireable(watermark int64) []Window {
	var out []Window
	for w := range m.windows {
		if watermark >= w.End+m.allowedLateness {
			out = append(out, w)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}

// Fire 触发并移除窗口，返回其聚合结果（按 Key 排序保证确定性）。
func (m *WindowManager) Fire(w Window) []WindowResult {
	keys, ok := m.windows[w]
	if !ok {
		return nil
	}
	delete(m.windows, w)
	out := make([]WindowResult, 0, len(keys))
	for k, st := range keys {
		out = append(out, WindowResult{Window: w, Key: k, Sum: st.sum, Count: st.count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Snapshot 导出未触发窗口的可序列化状态（用于检查点）。
func (m *WindowManager) Snapshot() map[Window]map[string]WindowStateSnapshot {
	out := make(map[Window]map[string]WindowStateSnapshot, len(m.windows))
	for w, keys := range m.windows {
		ks := make(map[string]WindowStateSnapshot, len(keys))
		for k, st := range keys {
			seen := make([]string, 0, len(st.seen))
			for id := range st.seen {
				seen = append(seen, id)
			}
			sort.Strings(seen)
			ks[k] = WindowStateSnapshot{Sum: st.sum, Count: st.count, Seen: seen}
		}
		out[w] = ks
	}
	return out
}

// Restore 从检查点快照恢复未触发窗口状态。
func (m *WindowManager) Restore(snap map[Window]map[string]WindowStateSnapshot) {
	for w, keys := range snap {
		ks := make(map[string]*windowState, len(keys))
		for k, s := range keys {
			st := &windowState{sum: s.Sum, count: s.Count, seen: make(map[string]struct{}, len(s.Seen))}
			for _, id := range s.Seen {
				st.seen[id] = struct{}{}
			}
			ks[k] = st
		}
		m.windows[w] = ks
	}
}
