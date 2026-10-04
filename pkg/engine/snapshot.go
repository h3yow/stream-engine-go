package engine

import (
	"sort"
	"time"

	"github.com/h3yow/stream-engine-go/pkg/checkpoint"
	"github.com/h3yow/stream-engine-go/pkg/event"
	"github.com/h3yow/stream-engine-go/pkg/watermark"
)

// Snapshot 为引擎运行期状态快照，供排障与验收使用。
//
// 字段含义稳定（语义不随版本变化）：
//   - Watermark：当前全局事件时间水位线；零值表示尚未形成水位线；
//   - MaxSeq：已处理事件的最大序号；
//   - Partitions：各数据源分片的供数状态与水位线进展（含 idle/finished 判定）；
//   - PendingWindows：已打开但尚未定稿的窗口（等待水位线越过定稿线）；
//   - CommittedWindows：已定稿并提交下游的窗口（含检查点恢复出的部分）；
//   - Drops：被拒绝事件按原因的精确计数，键集合固定为全部三种原因；
//   - Stats：引擎累计计数（与 Drops 中对应项一致）。
type Snapshot struct {
	Watermark        time.Time
	MaxSeq           int64
	Partitions       []watermark.PartitionInfo
	PendingWindows   []checkpoint.WindowRef
	CommittedWindows []checkpoint.WindowRef
	Drops            map[event.DropReason]int64
	Stats            Stats
}

// Snapshot 拍摄当前状态快照；可在运行中随时调用，不阻塞事件处理管线。
func (e *Engine) Snapshot() Snapshot {
	e.openMu.Lock()
	pending := make([]checkpoint.WindowRef, 0, len(e.openRefs))
	for ref := range e.openRefs {
		pending = append(pending, ref)
	}
	e.openMu.Unlock()

	e.committedMu.Lock()
	committed := make([]checkpoint.WindowRef, 0, len(e.committed))
	for ref := range e.committed {
		committed = append(committed, ref)
	}
	e.committedMu.Unlock()

	less := func(refs []checkpoint.WindowRef) {
		sort.Slice(refs, func(i, j int) bool {
			if refs[i].Key != refs[j].Key {
				return refs[i].Key < refs[j].Key
			}
			return refs[i].Start.Before(refs[j].Start)
		})
	}
	less(pending)
	less(committed)

	return Snapshot{
		Watermark:        e.wm.Current(),
		MaxSeq:           e.maxSeq.Load(),
		Partitions:       e.wm.Partitions(),
		PendingWindows:   pending,
		CommittedWindows: committed,
		Drops: map[event.DropReason]int64{
			event.ReasonLateBeyondAllowed: e.lateDropped.Load(),
			event.ReasonBackpressureFull:  e.backpressureDropped.Load(),
			event.ReasonPartitionFinished: e.partitionFinishedDropped.Load(),
		},
		Stats: e.Stats(),
	}
}
