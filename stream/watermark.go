package stream

import "math"

// WatermarkTracker 按“各注入分区最小最大事件时间 - 乱序容忍”推进水位线。
//
// 水位线语义：引擎认为事件时间 <= 水位线 的数据已基本到齐；
// 再叠加允许迟到（allowedLateness）决定窗口何时最终关闭。
//
// 多分区（多注入协程）时，水位线取所有活跃分区的最小值：
// 慢分区会拖住水位线，快分区的事件时间不会误伤慢分区的迟到事件，
// 因此聚合结果与注入并发度无关。水位线只增不减。
type WatermarkTracker struct {
	outOfOrderness int64
	maxSeen        []int64 // 每个分区已见到的最大事件时间
	finished       []bool  // 分区是否已耗尽
	active         int     // 未耗尽分区数
	watermark      int64
}

// NewWatermarkTracker 创建水位线跟踪器，partitions 为注入分区数（>=1）。
func NewWatermarkTracker(outOfOrderness int64, partitions int) *WatermarkTracker {
	if partitions <= 0 {
		partitions = 1
	}
	maxSeen := make([]int64, partitions)
	for i := range maxSeen {
		maxSeen[i] = math.MinInt64 // 未观察到的分区把水位线压在原点
	}
	return &WatermarkTracker{
		outOfOrderness: outOfOrderness,
		maxSeen:        maxSeen,
		finished:       make([]bool, partitions),
		active:         partitions,
	}
}

// Observe 观察分区 p 的一个事件时间，返回推进后的水位线（只增不减）。
func (t *WatermarkTracker) Observe(p int, eventTime int64) int64 {
	if eventTime > t.maxSeen[p] {
		t.maxSeen[p] = eventTime
	}
	return t.recompute()
}

// Finish 标记分区 p 已耗尽：它不再参与最小值计算。
func (t *WatermarkTracker) Finish(p int) {
	if t.finished[p] {
		return
	}
	t.finished[p] = true
	t.active--
	t.recompute()
}

// recompute 重算水位线：所有活跃分区最大事件时间的最小值减去乱序容忍。
func (t *WatermarkTracker) recompute() int64 {
	if t.active == 0 {
		return t.watermark
	}
	minMax := int64(math.MaxInt64)
	for p := range t.maxSeen {
		if !t.finished[p] && t.maxSeen[p] < minMax {
			minMax = t.maxSeen[p]
		}
	}
	if minMax != math.MaxInt64 && minMax != math.MinInt64 {
		// minMax==MinInt64 表示活跃分区均未观察到事件，水位线保持不动
		// （也避免 MinInt64-outOfOrderness 溢出回绕）。
		if w := minMax - t.outOfOrderness; w > t.watermark {
			t.watermark = w
		}
	}
	return t.watermark
}

// Watermark 返回当前水位线。
func (t *WatermarkTracker) Watermark() int64 { return t.watermark }

// MaxSeen 返回各分区已见到的最大事件时间（用于检查点）。
func (t *WatermarkTracker) MaxSeen() []int64 {
	out := make([]int64, len(t.maxSeen))
	copy(out, t.maxSeen)
	return out
}

// restoreMaxSeen 从检查点恢复各分区最大事件时间。
func (t *WatermarkTracker) restoreMaxSeen(maxSeen []int64) {
	copy(t.maxSeen, maxSeen)
	t.recompute()
}
