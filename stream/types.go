// Package stream 实现一个本地事件流处理引擎：
// 事件时间窗口、水位线推进、检查点恢复与背压控制。
package stream

// Event 是一条输入事件。EventTime 为事件时间（毫秒）。
type Event struct {
	ID        string // 全局唯一，用于幂等去重
	Key       string // 分组键
	EventTime int64  // 事件时间（毫秒时间戳）
	Value     int64  // 参与聚合的数值
}

// Offset 表示事件在源中的位点。
type Offset int64

// RejectReason 区分事件被拒绝/丢弃的原因。
type RejectReason string

const (
	// RejectLateBeyondAllowed 事件时间落后于水位线超过允许迟到范围。
	RejectLateBeyondAllowed RejectReason = "late_beyond_allowed"
	// RejectDuplicate 同一事件 ID 已被计入过，重复投递被去重。
	RejectDuplicate RejectReason = "duplicate"
	// RejectDroppedBackpressure 下游背压导致缓冲满，按丢弃策略丢弃。
	RejectDroppedBackpressure RejectReason = "dropped_backpressure"
)

// Rejection 记录一次被拒绝/丢弃的事件及原因。
type Rejection struct {
	Event  Event
	Reason RejectReason
}

// Window 表示一个事件时间窗口 [Start, End)。
type Window struct {
	Start int64
	End   int64
}

// Contains 判断事件时间是否落在窗口内。
func (w Window) Contains(ts int64) bool { return ts >= w.Start && ts < w.End }

// WindowResult 是一个窗口对一个 Key 的最终聚合输出。
type WindowResult struct {
	Window Window
	Key    string
	Sum    int64
	Count  int64
}

// Counters 汇总引擎运行期间的可观测计数。
type Counters struct {
	Received   int64 // 从源读取到的事件数
	Accepted   int64 // 被计入窗口的事件数（含允许范围内的迟到事件）
	Duplicates int64 // 重复投递被去重的事件数
	Rejected   int64 // 因超出允许迟到范围被拒绝的事件数
	Dropped    int64 // 因背压按丢弃策略丢弃的事件数
	Blocked    int64 // 因背压被阻塞的注入次数
	Emitted    int64 // 输出到下游的窗口结果数
}
