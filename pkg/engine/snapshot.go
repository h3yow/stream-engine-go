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
// 快照是同一时刻的一致视图（拍摄方式见 Snapshot 方法）：
// 水位线、各分片进展与状态、待定/已定稿窗口清单、按原因的拒绝计数
// 都对应同一处理前缀，彼此之间不会自相矛盾（例如不会出现“计入计数已加、
// 对应窗口却不在待定清单中”，或同一窗口同时出现在两个清单里）。
//
// 字段含义稳定（语义不随版本变化）：
//   - Watermark：当前全局事件时间水位线；零值表示尚未形成水位线；
//   - MaxSeq：已处理事件的最大序号；
//   - Partitions：各数据源分片的供数状态与水位线进展（含 idle/finished 判定
//     与终止位点 FinishSeq）；
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

// Snapshot 拍摄当前状态快照；可在运行中随时调用。
//
// 运行期间：经路由线程走与检查点相同的两阶段对齐协议（见 barrierCut），
// 在一致割切上读取——水位线、分片状态、待定/已定稿窗口清单与拒绝计数
// 都对应同一处理割切，并发反复读取也稳定、可直接核对。
// 引擎未启动或已关闭：处理管线已静止，直接读取即是一致视图。
func (e *Engine) Snapshot() Snapshot {
	if e.started.Load() && !e.closed.Load() {
		req := snapRequest{reply: make(chan Snapshot, 1)}
		select {
		case e.snapReq <- req:
			select {
			case snap := <-req.reply:
				return snap
			case <-e.quit:
			}
		case <-e.quit:
		}
		// 关停竞态：等待处理管线完全静止后再直接读取。
		e.routerWg.Wait()
		e.shardWg.Wait()
	}
	return e.collectSnapshot()
}

// collectSnapshot 读取当前状态，水位线取跟踪器当前值。
// 调用前提：处理管线静止（对齐割切之后或已关停）。
func (e *Engine) collectSnapshot() Snapshot {
	return e.collectSnapshotAt(e.wm.Current())
}

// collectSnapshotAt 以指定割切水位线读取当前状态（两阶段对齐协议专用）。
func (e *Engine) collectSnapshotAt(wm time.Time) Snapshot {
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
		Watermark:        wm,
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
