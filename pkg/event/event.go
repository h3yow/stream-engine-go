// Package event 定义流处理引擎使用的核心事件类型与丢弃原因。
package event

import "time"

// Event 是流经引擎的单条事件。
//
// Timestamp 为事件时间（由数据源产生），与处理时间无关；
// Seq 为事件在数据源中的唯一序号，用于重复投递识别与断点恢复。
type Event struct {
	Key       string
	Timestamp time.Time
	Value     int64
	Seq       int64
}

// DropReason 是事件被引擎拒绝/丢弃的可区分原因。
type DropReason string

const (
	// ReasonLateBeyondAllowed 表示事件迟到且超出允许迟到范围，窗口已提交清除。
	ReasonLateBeyondAllowed DropReason = "late_beyond_allowed_lateness"
	// ReasonBackpressureFull 表示下游持续阻塞、入口缓冲已满，按有界策略丢弃。
	ReasonBackpressureFull DropReason = "backpressure_buffer_full"
)

// DropRecord 记录一条被拒绝的事件及其原因，供计数与审计解释。
type DropRecord struct {
	Event  Event
	Reason DropReason
}
